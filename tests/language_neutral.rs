use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use voice_live::agent::{
    AgentTurnRequest, AgentTurnStream, ChannelAgentTurnStream, CognitiveAgent, CognitiveEvent,
    TurnResolution,
};
use voice_live::semantics::text::{ConfiguredTextAnalyzer, TextAnalyzer};
use voice_live::speech::render::{ConfiguredRenderer, InvocationProof, SpeechRenderer};
use voice_live::tts::{StreamingTts, TtsRequest, TtsStream};
use voice_live::{Revision, TurnId, VoiceEvent, VoiceRuntimeConfig, VoiceSessionBuilder};

fn config() -> VoiceRuntimeConfig {
    VoiceRuntimeConfig::from_yaml_str(
        r#"
observability: {event_log: false}
speech:
  outcomes:
    committed:
      operation: create
      class: transactional
      receipt_fields: {status: committed}
      bindings: [target]
"#,
    )
    .unwrap()
}

fn config_with_failure_text() -> VoiceRuntimeConfig {
    let mut cfg = config();
    cfg.speech.failure_text = "Please say that again.".into();
    cfg.tools
        .effects
        .insert("lookup".into(), voice_live::config::ToolEffect::PureRead);
    cfg
}

fn proof() -> InvocationProof {
    InvocationProof {
        record_id: "record-1".into(),
        call_id: "call-1".into(),
        operation: "create".into(),
        source: json!("model"),
        arguments: json!({"target":"A"}),
        output: json!({"status":"committed","target":"A"}).to_string(),
        executed: true,
        success: true,
        timed_out: false,
        cancelled: false,
        output_truncated: false,
    }
}
const PROPOSAL: &str = r#"{"text":"Created A.","outcome":"committed"}"#;

#[test]
fn neutral_rules_preserve_unknown_and_explicit_rules_are_not_language_modes() {
    let neutral = ConfiguredTextAnalyzer::new(&Default::default()).unwrap();
    for text in [
        "아니 토요일 말고 금요일",
        "stop please",
        "予約を変更",
        "Your reservation is confirmed.",
    ] {
        let analysis = neutral.user(text);
        assert!(analysis.cues.is_empty());
        assert!(!analysis.complete && !analysis.incomplete && !analysis.backchannel);
    }
    let cfg = VoiceRuntimeConfig::from_yaml_str(
        r#"
turn_control:
  hard_stop_phrases: ["stop please"]
  backchannel_words: ["I understand completely", "stop please"]
  complete_endings: ["finished"]
  text_rules:
    correction: ["instead of"]
"#,
    )
    .unwrap();
    let analyzer = ConfiguredTextAnalyzer::new(&cfg.turn_control).unwrap();
    assert!(analyzer.user("I understand completely").backchannel);
    assert!(analyzer.user("I understand completely,").backchannel);
    assert!(!analyzer.user("...").backchannel);
    let policy = voice_live::interaction::policy::ReflexPolicy::from_config(&cfg.turn_control);
    assert_eq!(
        policy.classify_overlap("stop please", "", &cfg.turn_control, &cfg.echo),
        voice_live::interaction::policy::OverlapOutcome::Interruption
    );
    // Removed speech keys fail loudly instead of being ignored.
    for removed in [
        "speech:\n  mode: legacy",
        "speech:\n  responses: {}",
        "speech:\n  mock_response: Hi",
    ] {
        assert!(
            VoiceRuntimeConfig::from_yaml_str(removed).is_err(),
            "{removed}"
        );
    }
    assert!(
        VoiceRuntimeConfig::from_yaml_str("turn_control:\n  text_rules:\n    correction: ['[']")
            .is_err()
    );
    assert!(
        VoiceRuntimeConfig::from_yaml_str("turn_control:\n  text_rules:\n    correction: ['a*']")
            .is_err()
    );
    assert!(
        VoiceRuntimeConfig::from_yaml_str("turn_control:\n  text_rules:\n    correction: ['\\b']")
            .is_err()
    );
}

#[test]
fn strict_renderer_checks_outcome_against_executed_records_without_model_ids() {
    let renderer = ConfiguredRenderer::new(&config().speech.outcomes).unwrap();
    let reply = renderer.render(PROPOSAL, &[proof()]).unwrap();
    assert_eq!(reply.text, "Created A.");
    assert_eq!(reply.class, voice_live::speech::ClaimClass::Transactional);
    assert_eq!(reply.records, ["record-1"]);
    // The record is found by operation; the model never supplies a call id.
    let mut other_call = proof();
    other_call.call_id = "provider-generated".into();
    assert!(renderer.render(PROPOSAL, &[other_call]).is_ok());
    assert!(renderer.render(PROPOSAL, &[]).is_err());
    for mutate in [
        "operation",
        "target",
        "receipt",
        "skip",
        "truncated",
        "cancelled",
        "timeout",
        "not_executed",
        "failure",
    ] {
        let mut p = proof();
        match mutate {
            "operation" => p.operation = "cancel".into(),
            "target" => p.arguments = json!({"target":"B"}),
            "receipt" => p.output = json!({"status":"committed","target":"B"}).to_string(),
            "skip" => p.output = json!({"skipped":true}).to_string(),
            "truncated" => p.output_truncated = true,
            "cancelled" => p.cancelled = true,
            "timeout" => p.timed_out = true,
            "not_executed" => p.executed = false,
            _ => p.success = false,
        }
        assert!(renderer.render(PROPOSAL, &[p]).is_err(), "{mutate}");
    }
    let mut fallback = proof();
    fallback.record_id = "fallback".into();
    fallback.source = json!({"fallback":{"original_tool":"create"}});
    let mut failed = proof();
    failed.success = false;
    assert!(renderer.render(PROPOSAL, &[failed, fallback]).is_err());
    for raw in [
        "",
        "   ",
        r#"{"class":"phatic","text":"Payment completed"}"#,
        r#"{"response":"ask"}"#,
        r#"{"text":"Done","outcome":"unknown"}"#,
        r#"{"text":"","outcome":"committed"}"#,
    ] {
        assert!(renderer.render(raw, &[proof()]).is_err(), "{raw}");
    }
    // Reply text in any language passes through as general speech: it names no outcome, so it carries no business evidence.
    for raw in [
        "Pick a date.",
        "날짜를 알려 주세요.",
        "日付を教えてください。",
    ] {
        let reply = renderer.render(raw, &[]).unwrap();
        assert_eq!(reply.text, raw);
        assert_eq!(reply.class, voice_live::speech::ClaimClass::Phatic);
        assert!(reply.records.is_empty() && reply.question.is_none());
    }
    let hinted = renderer
        .render(r#"{"text":"Continue?","question":"yes_no"}"#, &[])
        .unwrap();
    assert_eq!(
        hinted.question,
        Some(voice_live::interaction::ExpectedAnswerType::YesNo)
    );
}

#[test]
fn reservation_receipt_binds_optional_backend_defaults_and_changed_requests() {
    let sample = VoiceRuntimeConfig::from_file(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("full-duplex-demo/configs/voice-runtime.yaml"),
    )
    .unwrap();
    let renderer = ConfiguredRenderer::new(&sample.speech.outcomes).unwrap();
    let mut p = proof();
    p.operation = "reserve_restaurant".into();
    p.arguments = json!({"restaurant_id":"A","date":"2026-10-20","time":"19:00","party_size":2,"customer_name":"Test","confirmed":true,"idempotency_key":"offline"});
    let mut receipt = p.arguments.clone();
    receipt["status"] = json!("committed");
    receipt["window_seat_requested"] = json!(false);
    p.output = receipt.to_string();
    let proposal = json!({"text":"Booked.","outcome":"reserved"}).to_string();
    assert!(renderer.render(&proposal, &[p.clone()]).is_ok());
    p.arguments["window_seat_requested"] = json!(true);
    assert!(renderer.render(&proposal, &[p.clone()]).is_err());
}

struct RecordingTts(Arc<Mutex<Vec<String>>>);
#[async_trait]
impl StreamingTts for RecordingTts {
    async fn synthesize(
        &self,
        request: TtsRequest,
        cancellation: CancellationToken,
    ) -> anyhow::Result<Box<dyn TtsStream>> {
        self.0.lock().push(request.text.clone());
        voice_live::tts::fake::FakeTts::new(24_000, 20, 1)
            .synthesize(request, cancellation)
            .await
    }
}

#[tokio::test(start_paused = true)]
async fn runtime_host_overrides_reach_real_sessions_and_factories_are_per_session() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct HostRenderer;
    impl SpeechRenderer for HostRenderer {
        fn render(
            &self,
            _: &str,
            _: &[InvocationProof],
        ) -> anyhow::Result<voice_live::speech::render::RenderedReply> {
            Ok(voice_live::speech::render::RenderedReply {
                text: "Host response.".into(),
                class: voice_live::speech::ClaimClass::Phatic,
                question: None,
                records: vec![],
            })
        }
    }
    struct HostAnalyzer(Arc<AtomicUsize>);
    impl TextAnalyzer for HostAnalyzer {
        fn user(&self, _: &str) -> voice_live::semantics::text::UserTextAnalysis {
            self.0.fetch_add(1, Ordering::SeqCst);
            Default::default()
        }
    }
    let factories = Arc::new(AtomicUsize::new(0));
    let calls = factories.clone();
    let analyses = Arc::new(AtomicUsize::new(0));
    let texts = Arc::new(Mutex::new(Vec::new()));
    let mut cfg = config();
    cfg.dev.allow_text_injection = true;
    cfg.speech.mock_reply = Some("ask".into());
    cfg.turn_control.slot_fallback = voice_live::config::SlotFallback::KoreanExample;
    let policies = voice_live::RuntimePolicies {
        interaction_brain: None,
        text_analyzer: Some(Arc::new(HostAnalyzer(analyses.clone()))),
        slot_extractor: Some(Arc::new(move || {
            calls.fetch_add(1, Ordering::SeqCst);
            Arc::new(voice_live::semantics::extractor::NoopSlotExtractor)
        })),
        speech_renderer: Some(Arc::new(HostRenderer)),
    };
    let runtime = voice_live::VoiceRuntime::build_with_policies(
        cfg,
        Some(voice_live::SpeechProviders {
            asr: Arc::new(voice_live::asr::fake::MockAsr::quiet()),
            tts: Arc::new(RecordingTts(texts.clone())),
        }),
        policies,
    )
    .await
    .unwrap();
    let mut sessions = Vec::new();
    let mut receivers = Vec::new();
    for _ in 0..2 {
        let (tx, rx) = mpsc::channel(256);
        receivers.push(rx);
        let session = runtime.create_session(tx).await.unwrap();
        runtime.inject_user_text(session.session_id, "request");
        sessions.push(session);
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(factories.load(Ordering::SeqCst), 2);
    assert!(analyses.load(Ordering::SeqCst) > 0);
    assert_eq!(*texts.lock(), ["Host response.", "Host response."]);
    for session in sessions {
        runtime.close_session(session.session_id).await;
    }
}

struct TestAgent {
    final_text: String,
    record: Option<InvocationProof>,
    stale: bool,
    fail: bool,
    /// Sent before the Final, after any proof.
    events: Vec<CognitiveEvent>,
    resolutions: Mutex<Vec<TurnResolution>>,
}
#[async_trait]
impl CognitiveAgent for TestAgent {
    async fn start_turn(
        &self,
        request: AgentTurnRequest,
    ) -> anyhow::Result<Box<dyn AgentTurnStream>> {
        let (tx, rx) = mpsc::channel(8);
        if let Some(proof) = &self.record {
            tx.send(CognitiveEvent::InvocationProof {
                task_id: request.task_id,
                thought_epoch: request.thought_epoch + u64::from(self.stale),
                proof: proof.clone(),
            })
            .await
            .unwrap();
        }
        for event in &self.events {
            tx.send(event.clone()).await.unwrap();
        }
        tx.send(if self.fail {
            CognitiveEvent::Failed {
                message: "offline failure".into(),
            }
        } else {
            CognitiveEvent::Final {
                text: self.final_text.clone(),
            }
        })
        .await
        .unwrap();
        Ok(Box::new(ChannelAgentTurnStream::new(rx)))
    }
    fn resolve_turn(&self, _: voice_live::TaskId, resolution: TurnResolution) {
        self.resolutions.lock().push(resolution);
    }
}

async fn exercise(
    final_text: &str,
    record: Option<InvocationProof>,
    stale: bool,
    fail: bool,
) -> (Vec<String>, Vec<TurnResolution>) {
    exercise_with(config(), final_text, record, stale, fail).await
}

async fn exercise_with(
    cfg: VoiceRuntimeConfig,
    final_text: &str,
    record: Option<InvocationProof>,
    stale: bool,
    fail: bool,
) -> (Vec<String>, Vec<TurnResolution>) {
    exercise_events(cfg, final_text, record, stale, fail, Vec::new()).await
}

async fn exercise_events(
    cfg: VoiceRuntimeConfig,
    final_text: &str,
    record: Option<InvocationProof>,
    stale: bool,
    fail: bool,
    events: Vec<CognitiveEvent>,
) -> (Vec<String>, Vec<TurnResolution>) {
    let texts = Arc::new(Mutex::new(Vec::new()));
    let agent = Arc::new(TestAgent {
        final_text: final_text.into(),
        record,
        stale,
        fail,
        events,
        resolutions: Mutex::new(Vec::new()),
    });
    let shutdown = CancellationToken::new();
    let (session, handle) = VoiceSessionBuilder::new(
        Arc::new(cfg),
        Arc::new(voice_live::playback::UnacknowledgedPlaybackSink),
        Arc::new(RecordingTts(texts.clone())),
        agent.clone(),
    )
    .with_shutdown(shutdown.clone())
    .build();
    let meta = session.meta_factory.new_meta();
    let worker = tokio::spawn(session.run());
    handle
        .emit(VoiceEvent::UserTurnCommitted {
            meta,
            turn_id: TurnId::new(),
            transcript_revision: Revision(1),
            transcript: "request".into(),
        })
        .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    shutdown.cancel();
    worker.await.unwrap().unwrap();
    (texts.lock().clone(), agent.resolutions.lock().clone())
}

#[tokio::test(start_paused = true)]
async fn strict_rejection_blocks_tts_and_resolves_original_final_once() {
    for raw in [
        "",
        r#"{"class":"factual","text":"Done"}"#,
        r#"{"response":"ask"}"#,
        r#"{"text":"Done","outcome":"unknown"}"#,
        PROPOSAL,
    ] {
        let (texts, resolutions) = exercise(raw, None, false, false).await;
        assert!(texts.is_empty(), "TTS received unproven content: {texts:?}");
        assert_eq!(resolutions.len(), 1);
        assert!(
            matches!(&resolutions[0], TurnResolution::Audible { final_text, heard } if final_text == raw && heard == voice_live::speech::ledger::MARKER_UNDELIVERED)
        );
    }
    // With failure text configured, a rejected reply is not silence.
    let (texts, resolutions) =
        exercise_with(config_with_failure_text(), PROPOSAL, None, false, false).await;
    assert_eq!(texts, ["Please say that again."]);
    assert_eq!(resolutions.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn generated_reply_text_is_spoken_and_keeps_original_final_identity() {
    for raw in [
        "Pick a date.",
        r#"{"text":"Pick a date.","question":"free_text"}"#,
    ] {
        let (texts, resolutions) = exercise(raw, None, false, false).await;
        assert_eq!(texts, ["Pick a date."]);
        assert_eq!(resolutions.len(), 1);
        assert!(
            matches!(&resolutions[0], TurnResolution::Audible { final_text, .. } if final_text == raw)
        );
    }
}

#[tokio::test(start_paused = true)]
async fn unconfirmed_write_replaces_any_reply_with_failure_text() {
    let mut failed = proof();
    failed.success = false;
    // The reply claims success in plain words and names no outcome; the failed write in the same turn keeps it from being spoken.
    let (texts, resolutions) = exercise_with(
        config_with_failure_text(),
        "Your booking is confirmed.",
        Some(failed),
        false,
        false,
    )
    .await;
    assert_eq!(texts, ["Please say that again."]);
    assert!(
        matches!(&resolutions[..], [TurnResolution::Audible { heard, .. }] if heard == voice_live::speech::ledger::MARKER_UNDELIVERED)
    );
    // A failed read declared pure_read does not block the model's own words.
    let mut lookup = proof();
    lookup.operation = "lookup".into();
    lookup.success = false;
    let (texts, _) = exercise_with(
        config_with_failure_text(),
        "Nothing was found for that date.",
        Some(lookup),
        false,
        false,
    )
    .await;
    assert_eq!(texts, ["Nothing was found for that date."]);
}

#[tokio::test(start_paused = true)]
async fn a_call_to_an_unregistered_tool_does_not_count_as_a_write() {
    // The model asked for a tool that does not exist.
    // The framework reports a failed result, but nothing ran, so nothing can have been written.
    let unknown = CognitiveEvent::ToolCompleted {
        tool: "search_reservation_availability".into(),
        success: false,
        output: "Tool error: not registered".into(),
    };
    let (texts, _) = exercise_events(
        config_with_failure_text(),
        "I could not check that right now.",
        None,
        false,
        false,
        vec![unknown],
    )
    .await;
    assert_eq!(texts, ["I could not check that right now."]);
}

#[tokio::test(start_paused = true)]
async fn only_current_proof_admits_rendered_speech_not_model_json() {
    let (texts, resolutions) = exercise(PROPOSAL, Some(proof()), false, false).await;
    assert_eq!(texts, ["Created A."]);
    assert_eq!(resolutions.len(), 1);
    assert!(
        matches!(&resolutions[0], TurnResolution::Audible { final_text, .. } if final_text == PROPOSAL)
    );
    let (texts, _) = exercise(PROPOSAL, Some(proof()), true, false).await;
    assert!(texts.is_empty());
}

#[tokio::test(start_paused = true)]
async fn neutral_agent_failure_has_no_implicit_spoken_language() {
    let (texts, resolutions) = exercise("unused", None, false, true).await;
    assert!(texts.is_empty());
    assert_eq!(resolutions, [TurnResolution::Discard]);
}

struct ExpectedLanguage(&'static str);
impl tokio_tungstenite::tungstenite::handshake::server::Callback for ExpectedLanguage {
    fn on_request(
        self,
        req: &tokio_tungstenite::tungstenite::handshake::server::Request,
        response: tokio_tungstenite::tungstenite::handshake::server::Response,
    ) -> Result<
        tokio_tungstenite::tungstenite::handshake::server::Response,
        tokio_tungstenite::tungstenite::handshake::server::ErrorResponse,
    > {
        assert!(
            req.uri()
                .query()
                .unwrap()
                .split('&')
                .any(|p| p == format!("language={}", self.0))
        );
        Ok(response)
    }
}

#[tokio::test]
async fn cartesia_asr_accepts_factory_and_session_selectors_and_sends_primary_code() {
    use futures::StreamExt;
    use voice_live::asr::{AsrDuplexLimits, AsrSessionConfig, StreamingAsr};
    for (locale, wire) in [
        ("fr-CA", "fr"),
        ("pt-BR", "pt"),
        ("ja", "ja"),
        ("zh-Hant-TW", "zh"),
        ("haw", "haw"),
        ("yue", "yue"),
        ("zz-ZZ", "zz"),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cfg = voice_live::config::AsrConfig {
            endpoint: format!("ws://{}/stt", listener.local_addr().unwrap()),
            model: "ink-whisper".into(),
            locale: locale.into(),
            manual_commit: true,
            ..Default::default()
        };
        let worker = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let callback = ExpectedLanguage(wire);
            let mut socket = tokio_tungstenite::accept_hdr_async(socket, callback)
                .await
                .unwrap();
            while let Some(Ok(message)) = socket.next().await {
                if message.is_close() {
                    break;
                }
            }
        });
        let provider =
            voice_live::asr::cartesia::CartesiaStreamingAsr::with_api_key(&cfg, "offline".into())
                .unwrap();
        let mut session = provider
            .open_duplex(
                AsrSessionConfig {
                    sample_rate_hz: 16000,
                    locale: locale.into(),
                    manual_commit: true,
                },
                AsrDuplexLimits {
                    sample_budget: 16000,
                    command_budget: 64,
                    write_timeout: Duration::from_secs(1),
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();
        session.control.close().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .unwrap()
            .unwrap();
    }
}
