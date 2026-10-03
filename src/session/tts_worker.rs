use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::events::VoiceEvent;
use crate::meta::MetaFactory;
use crate::speech::SpeechAct;
use crate::tts::{
    AudioNormalizer, StreamingTts, TtsDuplexSession, TtsEvent, TtsInputLimits, TtsRequest,
    TtsSessionOptions, TtsStream,
};

#[derive(Default)]
pub struct TtsCancellationRegistry {
    tokens: Mutex<HashMap<crate::ids::SpeechId, CancellationToken>>,
}

impl TtsCancellationRegistry {
    pub fn insert(&self, id: crate::ids::SpeechId, token: CancellationToken) {
        self.tokens.lock().insert(id, token);
    }

    pub fn remove(&self, id: &crate::ids::SpeechId) {
        self.tokens.lock().remove(id);
    }

    pub fn cancel(&self, id: &crate::ids::SpeechId) {
        if let Some(token) = self.tokens.lock().get(id) {
            token.cancel();
        }
    }

    pub fn cancel_all(&self) {
        for (_, token) in self.tokens.lock().drain() {
            token.cancel();
        }
    }
}

pub struct TtsWorkerSettings {
    pub language: String,
    pub voice: String,
    pub max_input_bytes: usize,
    pub request_timeout: Duration,
    pub open_timeout: Duration,
    pub preferred_sample_rate_hz: Option<u32>,
}

struct Response<'a> {
    act: &'a SpeechAct,
    token: &'a CancellationToken,
    shutdown: &'a CancellationToken,
    events: &'a mpsc::Sender<VoiceEvent>,
    meta: &'a Arc<dyn MetaFactory>,
    settings: &'a TtsWorkerSettings,
}

struct RegistryEntry<'a> {
    registry: &'a TtsCancellationRegistry,
    id: crate::ids::SpeechId,
    token: CancellationToken,
}

impl Drop for RegistryEntry<'_> {
    fn drop(&mut self) {
        self.token.cancel();
        self.registry.remove(&self.id);
    }
}

/// Delivers one complete authorized act per response through cache or duplex input/output.
pub async fn run_tts_worker(
    tts: Arc<dyn StreamingTts>,
    mut speech_rx: mpsc::Receiver<(SpeechAct, CancellationToken)>,
    event_tx: mpsc::Sender<VoiceEvent>,
    meta_factory: Arc<dyn MetaFactory>,
    shutdown: CancellationToken,
    registry: Arc<TtsCancellationRegistry>,
    settings: TtsWorkerSettings,
) {
    loop {
        let item = tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            item = speech_rx.recv() => match item { Some(item) => item, None => break },
        };
        let (act, token) = item;
        let _entry = RegistryEntry {
            registry: &registry,
            id: act.id,
            token: token.clone(),
        };
        if token.is_cancelled() {
            continue;
        }
        let response = Response {
            act: &act,
            token: &token,
            shutdown: &shutdown,
            events: &event_tx,
            meta: &meta_factory,
            settings: &settings,
        };
        if let Err(failure) = response.run(&tts).await {
            response.failure(failure.to_string());
        }
    }
    registry.cancel_all();
}

impl Response<'_> {
    async fn cancelled(&self) {
        tokio::select! { _ = self.token.cancelled() => {}, _ = self.shutdown.cancelled() => {} }
    }

    fn failure(&self, message: String) {
        if self.token.is_cancelled() || self.shutdown.is_cancelled() {
            return;
        }
        let event = VoiceEvent::TtsFailed {
            meta: self.meta.new_meta(),
            speech_id: self.act.id,
            speech_epoch: self.act.speech_epoch,
            recoverable: true,
            message,
        };
        if self.events.try_send(event).is_err() {
            tracing::error!("TTS terminal event queue overloaded; terminating session");
            self.shutdown.cancel();
        }
    }

    async fn run(&self, tts: &Arc<dyn StreamingTts>) -> anyhow::Result<()> {
        anyhow::ensure!(!self.act.text.trim().is_empty(), "tts input is empty");
        anyhow::ensure!(
            self.act.text.len() <= self.settings.max_input_bytes,
            "tts input exceeded byte budget"
        );
        let request = TtsRequest {
            speech_id: self.act.id,
            speech_epoch: self.act.speech_epoch,
            text: self.act.text.clone(),
            language: self.settings.language.clone(),
            voice: self.settings.voice.clone(),
            claim_class: self.act.class,
        };
        let cached = tokio::select! {
            biased;
            _ = self.cancelled() => anyhow::bail!("tts cancelled"),
            result = tokio::time::timeout(self.settings.open_timeout, tts.try_cached_synthesis(&request, self.token.clone())) => result.map_err(|_| anyhow::anyhow!("tts cache lookup timed out"))?,
        };
        if let Some(events) = cached {
            return self.drain_bounded(events).await;
        }
        let options = TtsSessionOptions {
            speech_id: self.act.id,
            speech_epoch: self.act.speech_epoch,
            language: request.language,
            voice: request.voice,
            claim_class: self.act.class,
            preferred_sample_rate_hz: self.settings.preferred_sample_rate_hz,
        };
        let limits = TtsInputLimits {
            max_input_bytes: self.settings.max_input_bytes,
            request_timeout: self.settings.request_timeout,
        };
        let session = tokio::select! {
            biased;
            _ = self.cancelled() => anyhow::bail!("tts cancelled"),
            result = tokio::time::timeout(self.settings.open_timeout, tts.open_text_stream(options, limits, self.token.clone())) => result.map_err(|_| anyhow::anyhow!("tts open timed out"))??,
        };
        let TtsDuplexSession {
            mut input,
            mut events,
            mut control,
        } = session;
        // Input operations are nonblocking admissions; provider I/O runs independently of them.
        let admitted = if events.next_event().now_or_never().is_some() {
            Err(anyhow::anyhow!("tts output arrived before text admission"))
        } else {
            input
                .push_text(self.act.text.clone())
                .and_then(|()| input.finish_input())
                .map_err(anyhow::Error::from)
        };
        let result = match admitted {
            Ok(()) => {
                drop(input);
                self.drain_bounded(events).await
            }
            Err(error) => Err(anyhow::anyhow!(error)),
        };
        control.cancel();
        let _ = tokio::time::timeout(Duration::from_millis(1_500), control.close()).await;
        result
    }

    async fn drain_bounded(&self, events: Box<dyn TtsStream>) -> anyhow::Result<()> {
        tokio::select! {
            biased;
            _ = self.cancelled() => Err(anyhow::anyhow!("tts cancelled")),
            result = tokio::time::timeout(self.settings.request_timeout, self.drain(events)) => result.map_err(|_| anyhow::anyhow!("tts request timed out"))?,
        }
    }

    async fn drain(&self, mut events: Box<dyn TtsStream>) -> anyhow::Result<()> {
        let mut normalizer = AudioNormalizer::default();
        loop {
            let event = events
                .next_event()
                .await?
                .ok_or_else(|| anyhow::anyhow!("tts stream ended without terminal"))?;
            for event in normalizer.normalize(event)? {
                let (event, done) = match event {
                    TtsEvent::Audio {
                        sequence,
                        sample_rate,
                        pcm_s16le,
                        ..
                    } => (
                        VoiceEvent::TtsAudioChunk {
                            meta: self.meta.new_meta(),
                            speech_id: self.act.id,
                            speech_epoch: self.act.speech_epoch,
                            sequence,
                            sample_rate,
                            pcm: pcm_s16le,
                        },
                        false,
                    ),
                    TtsEvent::AudioDone { .. } => (
                        VoiceEvent::TtsAudioDone {
                            meta: self.meta.new_meta(),
                            speech_id: self.act.id,
                            speech_epoch: self.act.speech_epoch,
                        },
                        true,
                    ),
                    TtsEvent::Error { message, .. } => anyhow::bail!(message),
                };
                self.events
                    .send(event)
                    .await
                    .map_err(|_| anyhow::anyhow!("session events closed"))?;
                if done {
                    return Ok(());
                }
            }
        }
    }
}
