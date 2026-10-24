//! A full-duplex voice interaction runtime.
//!
//! voice-live connects a streaming speech recognizer, an agent, and a streaming speech synthesizer so an assistant can listen while it speaks.
//! It ducks playback the moment the user starts talking, lets a semantic interpretation decide whether to resume, abort, or stop, and rewrites the agent's memory to what the user actually heard.
//!
//! The library owns no transport. A host builds a [`VoiceRuntime`], opens a session with [`VoiceRuntime::create_session`], and bridges the session's channels to its client: [`ClientInput`] goes in, [`ServerMessage`] comes out.
//!
//! ```no_run
//! use voice_live::{ClientControlMessage, ClientInput, ServerMessage, VoiceRuntime, VoiceRuntimeConfig};
//!
//! # async fn run() -> anyhow::Result<()> {
//! // Development providers: typed text instead of speech recognition, silent audio instead of synthesis.
//! let config = VoiceRuntimeConfig::from_yaml_str(
//!     "asr: {provider: inject}\ntts: {provider: mock}\ndev: {allow_text_injection: true}",
//! )?;
//! let runtime = VoiceRuntime::build(config).await?;
//!
//! let (client_tx, mut client_rx) = tokio::sync::mpsc::channel::<ServerMessage>(256);
//! let session = runtime.create_session(client_tx).await?;
//! session
//!     .input_tx
//!     .send(ClientInput::Control(ClientControlMessage::SessionStart {
//!         input_sample_rate: 16_000,
//!         channels: 1,
//!     }))
//!     .await?;
//! runtime.inject_user_text(session.session_id, "I'd like to book a table.");
//!
//! while let Some(message) = client_rx.recv().await {
//!     // Forward JSON control messages and binary PCM to the client, and send its playback acknowledgements back through `session.input_tx`.
//! #   let _ = message;
//! #   break;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Where to start
//!
//! - [`VoiceRuntimeConfig`] is the YAML configuration; every key is documented in the repository's `docs/configuration.md`.
//! - [`VoiceRuntime`] builds providers and agents and hands out sessions. [`SpeechProviders`] replaces the built-in speech providers, and [`RuntimePolicies`] supplies tools and replaces interpretation or rendering policies.
//! - [`protocol`] defines the client messages a transport carries.
//! - [`agent::CognitiveAgent`], [`asr::StreamingAsr`], [`tts::StreamingTts`], and [`speech::render::SpeechRenderer`] are the extension traits.
//!
//! # Features
//!
//! - `framework` (default): agents built from ai-agents YAML specs.
//! - `cartesia`, `together`, `qwen` (default): the built-in network speech providers.
//!
//! # Testing utilities
//!
//! [`agent::mock`], [`asr::fake`], [`tts::fake`], and the [`Scenario`] runner are public so hosts can drive the runtime in virtual time without network access.
//! The Korean resources reachable through [`config::TurnControlConfig::korean_example`] and `turn_control.slot_fallback: korean_example` are an opt-in compatibility example for the deterministic rule path, not a default.

pub mod agent;
pub mod asr;
pub mod clock;
pub mod config;
pub mod echo;
pub mod epochs;
pub mod events;
pub mod ids;
pub mod interaction;
pub mod logging;
pub mod media;
pub mod meta;
pub mod metrics;
pub mod playback;
pub mod protocol;
pub mod provider;
pub mod replay;
pub mod runtime;
pub mod scenario;
pub mod semantics;
pub mod session;
pub mod speech;
pub mod transcript;
pub mod tts;
pub mod vad;

pub use config::VoiceRuntimeConfig;
pub use events::VoiceEvent;
pub use ids::{Revision, SessionId, SpeechId, TaskId, TurnId};
pub use protocol::{ClientControlMessage, ServerMessage};
pub use runtime::{ClientInput, GatewaySession, RuntimePolicies, SpeechProviders, VoiceRuntime};
pub use scenario::{Scenario, ScenarioReport, ScenarioRunner};
pub use session::{
    DeepWorkRequest, DeepWorkResult, DeepWorker, VoiceSession, VoiceSessionBuilder,
    VoiceSessionHandle,
};
