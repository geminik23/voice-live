//! Offline Cartesia TTS wire and ownership contracts; no paid endpoints.
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use futures::{SinkExt, StreamExt};
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use voice_live::config::TtsConfig;
use voice_live::speech::ClaimClass;
use voice_live::tts::cartesia::CartesiaStreamingTts;
use voice_live::tts::{
    StreamingTts, TextInputMode, TtsDuplexSession, TtsEvent, TtsInputError, TtsInputLimits,
    TtsOpenError, TtsRequest, TtsSessionOptions,
};
use voice_live::{SpeechId, SpeechProviders, VoiceRuntime, VoiceRuntimeConfig};

#[derive(Clone, Copy)]
enum Reply {
    Audio,
    Hang,
    EarlyError,
    Error,
    ErrorDone,
    WrongContext,
    BadBase64,
    OddPcm,
    Oversize,
    DoneWithoutAudio,
    FlushOnly,
    Flood,
    FinalChunk,
    Binary,
    WrongFormat,
    Premature,
    BackpressuredPremature,
}

type Requests = Arc<Mutex<Vec<Value>>>;
struct Server {
    endpoint: String,
    requests: Requests,
    task: JoinHandle<()>,
}

struct Handshake {
    refusal: Option<&'static str>,
}
impl tokio_tungstenite::tungstenite::handshake::server::Callback for Handshake {
    fn on_request(
        self,
        request: &tokio_tungstenite::tungstenite::handshake::server::Request,
        response: tokio_tungstenite::tungstenite::handshake::server::Response,
    ) -> Result<
        tokio_tungstenite::tungstenite::handshake::server::Response,
        tokio_tungstenite::tungstenite::handshake::server::ErrorResponse,
    > {
        assert_eq!(request.headers()["authorization"], "Bearer test-secret");
        assert_eq!(request.headers()["cartesia-version"], "2026-08-14");
        assert!(request.uri().query().is_none());
        match self.refusal {
            Some(code) => Err(tokio_tungstenite::tungstenite::http::Response::builder()
                .status(429)
                .body(Some(
                    json!({"error_code":code, "message":"test-secret"}).to_string(),
                ))
                .unwrap()),
            None => Ok(response),
        }
    }
}

async fn server(reply: Reply) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}/tts/websocket", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = requests.clone();
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_hdr_async(socket, Handshake { refusal: None })
            .await
            .unwrap();
        if matches!(reply, Reply::EarlyError) {
            let _ = socket.send(tokio_tungstenite::tungstenite::Message::Text(json!({"type":"error", "status_code":429, "error_code":"quota_exceeded", "message":"test-secret"}).to_string().into())).await;
        }
        while let Some(Ok(message)) = socket.next().await {
            let tokio_tungstenite::tungstenite::Message::Text(text) = message else {
                break;
            };
            let request: Value =
                serde_json::from_str(&text).expect("JSON generation request, no session.create");
            recorded.lock().push(request.clone());
            let context = request["context_id"].as_str().unwrap();
            let continuing = request["continue"].as_bool().unwrap();
            let chunk = |data: Vec<u8>, done: bool| json!({"type":"chunk", "context_id":context, "done":done, "status_code":206, "data":base64::engine::general_purpose::STANDARD.encode(data), "step_time":1});
            let responses = match reply {
                Reply::Hang | Reply::EarlyError => vec![],
                Reply::Error | Reply::ErrorDone => vec![
                    json!({"type":"error", "status_code":400, "error_code":"model_not_found", "done":matches!(reply, Reply::ErrorDone), "message":"test-secret user transcript"}),
                ],
                Reply::WrongContext => vec![
                    json!({"type":"chunk", "context_id":"wrong", "done":false, "status_code":206, "data":"AAA="}),
                ],
                Reply::BadBase64 => vec![
                    json!({"type":"chunk", "context_id":context, "done":false, "status_code":206, "data":"!"}),
                ],
                Reply::OddPcm => vec![chunk(vec![0], false)],
                Reply::WrongFormat => {
                    let mut response = chunk(vec![0; 4], false);
                    response["output_format"] =
                        json!({"sample_rate":16000, "encoding":"pcm_f32le"});
                    vec![response]
                }
                Reply::Premature => vec![chunk(vec![0; 4], true)],
                Reply::BackpressuredPremature => {
                    if recorded.lock().len() == 1 {
                        (0..17)
                            .map(|index| chunk(vec![0; 4], index == 16))
                            .collect()
                    } else {
                        vec![]
                    }
                }
                Reply::Oversize => vec![chunk(vec![0; 65_538], false)],
                Reply::DoneWithoutAudio => vec![
                    json!({"type":"done", "context_id":context, "done":true, "status_code":206}),
                ],
                Reply::FlushOnly => vec![
                    json!({"type":"flush_done", "context_id":context, "done":true, "status_code":206}),
                ],
                Reply::Flood => (0..40).map(|_| chunk(vec![0; 8_192], false)).collect(),
                Reply::FinalChunk if !continuing => vec![chunk(vec![0; 10_000], true)],
                Reply::Audio | Reply::FinalChunk if continuing => {
                    vec![chunk(vec![0; 10_000], false)]
                }
                _ => vec![
                    json!({"type":"done", "context_id":context, "done":true, "status_code":206}),
                ],
            };
            if matches!(reply, Reply::Binary) {
                let _ = socket
                    .send(tokio_tungstenite::tungstenite::Message::Binary(
                        vec![0; 4].into(),
                    ))
                    .await;
            }
            for response in responses {
                if socket
                    .send(tokio_tungstenite::tungstenite::Message::Text(
                        response.to_string().into(),
                    ))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            if matches!(reply, Reply::Premature) {
                tokio::time::sleep(Duration::from_millis(100)).await;
                return;
            }
        }
    });
    Server {
        endpoint,
        requests,
        task,
    }
}

fn config(endpoint: &str) -> TtsConfig {
    TtsConfig {
        provider: "cartesia".into(),
        model: "sonic-3.6".into(),
        endpoint: endpoint.into(),
        voice: "test-voice".into(),
        api_key_env: "CARTESIA_API_KEY".into(),
        voice_env: "CARTESIA_VOICE_ID".into(),
        ..TtsConfig::default()
    }
}
fn options() -> TtsSessionOptions {
    TtsSessionOptions {
        speech_id: SpeechId::new(),
        speech_epoch: 1,
        language: "ko-KR".into(),
        voice: "test-voice".into(),
        claim_class: ClaimClass::Phatic,
        preferred_sample_rate_hz: Some(24_000),
    }
}
async fn open(server: &Server, bytes: usize, millis: u64) -> TtsDuplexSession {
    CartesiaStreamingTts::with_api_key(&config(&server.endpoint), "test-secret".into())
        .unwrap()
        .open_text_stream(
            options(),
            TtsInputLimits {
                max_input_bytes: bytes,
                request_timeout: Duration::from_millis(millis),
            },
            CancellationToken::new(),
        )
        .await
        .unwrap()
}
async fn terminal(session: &mut TtsDuplexSession) -> TtsEvent {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut previous = 0;
        while let Some(event) = session.events.next_event().await.unwrap() {
            match event {
                TtsEvent::Audio {
                    sequence,
                    sample_rate,
                    pcm_s16le,
                    ..
                } => {
                    assert!(sequence > previous);
                    previous = sequence;
                    assert_eq!(sample_rate, 24_000);
                    assert!(pcm_s16le.len() <= 8_192 && pcm_s16le.len().is_multiple_of(2));
                }
                terminal => return terminal,
            }
        }
        panic!("missing terminal");
    })
    .await
    .unwrap()
}
async fn close(mut session: TtsDuplexSession, server: Server) {
    session.control.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), server.task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn selectors_preserve_region_script_and_aliases_on_every_fragment() {
    for (alias, expected) in [
        ("en", "en"),
        ("en-US", "en-US"),
        ("en-GB", "en-GB"),
        ("English", "en"),
        ("fr-CA", "fr-CA"),
        ("pt-BR", "pt-BR"),
        ("zh-Hant-TW", "zh-Hant-TW"),
        ("yue", "yue"),
        ("zz-ZZ", "zz-ZZ"),
    ] {
        let server = server(Reply::Audio).await;
        let provider =
            CartesiaStreamingTts::with_api_key(&config(&server.endpoint), "test-secret".into())
                .unwrap();
        let mut session = provider
            .open_text_stream(
                TtsSessionOptions {
                    language: alias.into(),
                    ..options()
                },
                TtsInputLimits {
                    max_input_bytes: 1024,
                    request_timeout: Duration::from_secs(2),
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();
        session.input.push_text("English speech.".into()).unwrap();
        session.input.finish_input().unwrap();
        assert!(matches!(
            terminal(&mut session).await,
            TtsEvent::AudioDone { .. }
        ));
        assert!(
            server
                .requests
                .lock()
                .iter()
                .all(|request| request["language"] == expected)
        );
        close(session, server).await;
    }
    assert_eq!(TtsConfig::default().language, "Korean");
}

#[tokio::test]
async fn native_wire_streams_before_finish_and_preserves_fragments() {
    let server = server(Reply::Audio).await;
    let provider =
        CartesiaStreamingTts::with_api_key(&config(&server.endpoint), "test-secret".into())
            .unwrap();
    assert_eq!(provider.text_input_mode(), TextInputMode::Incremental);
    assert!(provider.supports_text_stream());
    let mut session = open(&server, 65_536, 2_000).await;
    session.input.push_text("토요일 ".into()).unwrap();
    for _ in 0..2 {
        assert!(matches!(
            session.events.next_event().await.unwrap(),
            Some(TtsEvent::Audio { .. })
        ));
    }
    session.input.push_text("일곱 시입니다.".into()).unwrap();
    session.input.finish_input().unwrap();
    session.input.finish_input().unwrap();
    assert!(matches!(
        session.input.push_text("late".into()),
        Err(TtsInputError::Finished)
    ));
    assert!(matches!(
        terminal(&mut session).await,
        TtsEvent::AudioDone { .. }
    ));
    assert!(session.events.next_event().await.unwrap().is_none());
    {
        let requests = server.requests.lock();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0]["transcript"], "토요일 ");
        assert_eq!(requests[1]["transcript"], "일곱 시입니다.");
        assert_eq!(requests[2]["transcript"], "");
        for (index, request) in requests.iter().enumerate() {
            assert_eq!(request["continue"], index != 2);
            assert_eq!(request["context_id"], requests[0]["context_id"]);
            assert_eq!(request["model_id"], "sonic-3.6");
            assert_eq!(request["voice"]["id"], "test-voice");
            assert_eq!(request["language"], "ko-KR");
            assert!(request.get("locale").is_none());
            assert_eq!(request["max_buffer_delay_ms"], 0);
            assert_eq!(
                request["output_format"],
                json!({"container":"raw", "encoding":"pcm_s16le", "sample_rate":24000})
            );
        }
    }
    close(session, server).await;
}

#[tokio::test]
async fn finish_reserves_capacity_and_input_drop_keeps_audio_drain() {
    let server = server(Reply::Audio).await;
    let mut session = open(&server, 65_536, 3_000).await;
    for _ in 0..64 {
        session.input.push_text("x".into()).unwrap();
    }
    assert!(
        matches!(session.input.push_text("returned".into()), Err(TtsInputError::Full(fragment)) if fragment == "returned")
    );
    session.input.finish_input().unwrap();
    drop(session.input);
    let mut bytes = 0;
    while let Some(event) = session.events.next_event().await.unwrap() {
        match event {
            TtsEvent::Audio { pcm_s16le, .. } => bytes += pcm_s16le.len(),
            TtsEvent::AudioDone { .. } => break,
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(bytes, 64 * 10_000);
    session.control.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), server.task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn contextless_errors_are_terminal_before_and_after_input() {
    for reply in [Reply::EarlyError, Reply::Error, Reply::ErrorDone] {
        let server = server(reply).await;
        let mut session = open(&server, 1024, 1_000).await;
        if !matches!(reply, Reply::EarlyError) {
            session.input.push_text("요청".into()).unwrap();
        }
        let event = terminal(&mut session).await;
        let TtsEvent::Error {
            message,
            recoverable,
        } = event
        else {
            panic!("error required")
        };
        assert!(!recoverable);
        assert!(message.contains("quota_exceeded") || message.contains("model_not_found"));
        assert!(!message.contains("test-secret") && !message.contains("user transcript"));
        close(session, server).await;
    }
}

#[tokio::test]
async fn malformed_audio_wrong_identity_and_nonterminal_flush_cannot_succeed() {
    for reply in [
        Reply::WrongContext,
        Reply::BadBase64,
        Reply::OddPcm,
        Reply::Oversize,
        Reply::DoneWithoutAudio,
        Reply::FlushOnly,
        Reply::Binary,
        Reply::WrongFormat,
    ] {
        let server = server(reply).await;
        let mut session = open(&server, 1024, 150).await;
        session.input.push_text("요청".into()).unwrap();
        session.input.finish_input().unwrap();
        let first = tokio::time::timeout(Duration::from_secs(2), session.events.next_event())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let TtsEvent::Error { message, .. } = first else {
            panic!("invalid output must fail before any audio: {first:?}");
        };
        if matches!(reply, Reply::WrongFormat) {
            assert!(message.contains("metadata mismatch"));
        }
        if matches!(reply, Reply::FlushOnly) {
            assert!(message.contains("timed out"));
        }
        close(session, server).await;
    }
}

#[tokio::test]
async fn final_chunk_drains_pcm_before_one_terminal() {
    let server = server(Reply::FinalChunk).await;
    let mut session = open(&server, 1024, 1_000).await;
    session.input.push_text("요청".into()).unwrap();
    session.input.finish_input().unwrap();
    let mut bytes = 0;
    while let Some(event) = session.events.next_event().await.unwrap() {
        match event {
            TtsEvent::Audio { pcm_s16le, .. } => bytes += pcm_s16le.len(),
            TtsEvent::AudioDone { .. } => break,
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(bytes, 20_000);
    assert!(session.events.next_event().await.unwrap().is_none());
    close(session, server).await;
}

#[tokio::test]
async fn owner_deadline_survives_unpolled_full_audio_queue() {
    let server = server(Reply::Flood).await;
    let mut session = open(&server, 1024, 100).await;
    session.input.push_text("요청".into()).unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(matches!(
        session.input.push_text("late".into()),
        Err(TtsInputError::Closed)
    ));
    assert!(
        matches!(terminal(&mut session).await, TtsEvent::Error { message, .. } if message.contains("timed out"))
    );
    close(session, server).await;
}

#[tokio::test]
async fn deadline_starts_at_nonempty_admission_and_never_extends() {
    let server = server(Reply::Hang).await;
    let mut session = open(&server, 1024, 150).await;
    session.input.push_text(String::new()).unwrap();
    assert!(matches!(
        session.input.finish_input(),
        Err(TtsInputError::EmptyInput)
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    session.input.push_text("첫 ".into()).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    session.input.push_text("둘".into()).unwrap();
    let event = tokio::time::timeout(Duration::from_millis(100), terminal(&mut session))
        .await
        .unwrap();
    assert!(matches!(event, TtsEvent::Error { message, .. } if message.contains("timed out")));
    close(session, server).await;
}

#[tokio::test]
async fn input_limits_and_drop_cancel_without_finish() {
    let server = server(Reply::Hang).await;
    let mut session = open(&server, 3, 1000).await;
    assert!(
        matches!(session.input.push_text("초과".into()), Err(TtsInputError::TooLarge(fragment)) if fragment == "초과")
    );
    session.input.push_text("   ".into()).unwrap();
    assert!(matches!(
        session.input.finish_input(),
        Err(TtsInputError::EmptyInput)
    ));
    drop(session.input);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), session.events.next_event())
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
    session.control.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), server.task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn expired_context_is_not_reopened_for_late_text() {
    let server = server(Reply::Audio).await;
    let mut session = open(&server, 1024, 3000).await;
    session.input.push_text("요청".into()).unwrap();
    for _ in 0..2 {
        session.events.next_event().await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(1100)).await;
    session.input.push_text("추가".into()).unwrap();
    assert!(
        matches!(terminal(&mut session).await, TtsEvent::Error { message, .. } if message.contains("expired"))
    );
    assert_eq!(server.requests.lock().len(), 1);
    close(session, server).await;
}

#[tokio::test]
async fn premature_terminal_cannot_discard_queued_text_after_finish_admission() {
    let server = server(Reply::Premature).await;
    let mut session = open(&server, 16_000_000, 3000).await;
    for _ in 0..64 {
        session.input.push_text("x".repeat(200_000)).unwrap();
    }
    session.input.finish_input().unwrap();
    assert!(matches!(
        terminal(&mut session).await,
        TtsEvent::Error { .. }
    ));
    assert!(server.requests.lock().len() < 65);
    close(session, server).await;
}

#[tokio::test]
async fn audio_backpressure_cannot_retroactively_authorize_a_premature_terminal() {
    let server = server(Reply::BackpressuredPremature).await;
    let mut session = open(&server, 1024, 2000).await;
    session.input.push_text("첫 요청".into()).unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    match session.input.push_text("추가 요청".into()) {
        Ok(()) => {
            session.input.finish_input().unwrap();
        }
        Err(TtsInputError::Closed) => {}
        other => panic!("unexpected admission: {other:?}"),
    }
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(matches!(
        terminal(&mut session).await,
        TtsEvent::Error { .. }
    ));
    close(session, server).await;
}

#[tokio::test]
async fn cancelled_output_poll_preserves_terminal_for_repoll() {
    let server = server(Reply::Hang).await;
    let mut session = open(&server, 1024, 100).await;
    session.input.push_text("요청".into()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), session.events.next_event())
            .await
            .is_err()
    );
    assert!(matches!(
        terminal(&mut session).await,
        TtsEvent::Error { .. }
    ));
    assert!(session.events.next_event().await.unwrap().is_none());
    close(session, server).await;
}

#[tokio::test]
async fn explicit_cancel_and_handle_drops_close_live_sockets() {
    for drop_kind in 0..3 {
        let server = server(Reply::Flood).await;
        let mut session = open(&server, 1024, 3000).await;
        session.input.push_text("요청".into()).unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        match drop_kind {
            0 => {
                session.control.cancel();
                tokio::time::timeout(Duration::from_secs(2), server.task)
                    .await
                    .unwrap()
                    .unwrap();
                session.control.close().await.unwrap();
            }
            1 => {
                drop(session.events);
                tokio::time::timeout(Duration::from_secs(2), server.task)
                    .await
                    .unwrap()
                    .unwrap();
                session.control.close().await.unwrap();
            }
            _ => {
                drop(session.control);
                tokio::time::timeout(Duration::from_secs(2), server.task)
                    .await
                    .unwrap()
                    .unwrap();
            }
        }
    }
    let server = server(Reply::Hang).await;
    let provider =
        CartesiaStreamingTts::with_api_key(&config(&server.endpoint), "test-secret".into())
            .unwrap();
    let stream = provider
        .synthesize(
            TtsRequest {
                speech_id: SpeechId::new(),
                speech_epoch: 1,
                text: "요청".into(),
                language: "ko".into(),
                voice: "test-voice".into(),
                claim_class: ClaimClass::Phatic,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    tokio::task::yield_now().await;
    drop(stream);
    tokio::time::timeout(Duration::from_secs(2), server.task)
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn validates_factory_only_and_does_not_echo_credentials() {
    for endpoint in [
        "ws://localhost/tts?cartesia_version=x",
        "ws://localhost/tts?access_token=secret",
        "ws://user:secret@localhost/tts",
        "ws://example.com/tts",
    ] {
        let result = CartesiaStreamingTts::with_api_key(&config(endpoint), "secret".into());
        assert!(result.is_err());
        assert!(!result.err().unwrap().to_string().contains("secret"));
    }
    let mut config = config("ws://localhost/tts");
    config.cartesia.max_buffer_delay_ms = 5001;
    assert!(CartesiaStreamingTts::with_api_key(&config, "secret".into()).is_err());
    config.cartesia.max_buffer_delay_ms = 0;
    config.cartesia.api_version = "invalid".into();
    assert!(CartesiaStreamingTts::with_api_key(&config, "secret".into()).is_err());
    let default = TtsConfig::default();
    assert_eq!(default.cartesia.api_version, "2026-08-14");
    assert_eq!(default.cartesia.max_buffer_delay_ms, 0);
}

#[tokio::test]
async fn cartesia_native_warmup_is_cached_without_opening_a_second_socket() {
    let server = server(Reply::Audio).await;
    let provider =
        CartesiaStreamingTts::with_api_key(&config(&server.endpoint), "test-secret".into())
            .unwrap();
    let cached = voice_live::tts::premade::PremadeTts::with_rate(
        Arc::new(provider),
        vec!["네".into()],
        "ko".into(),
        "test-voice".into(),
        Some(24000),
    );
    cached
        .warmup_with_limits(Duration::from_secs(2), 1024)
        .await;
    assert_eq!(cached.text_input_mode(), TextInputMode::Incremental);
    let request = TtsRequest {
        speech_id: SpeechId::new(),
        speech_epoch: 1,
        text: "네".into(),
        language: "ko".into(),
        voice: "test-voice".into(),
        claim_class: ClaimClass::Phatic,
    };
    let mut stream = cached
        .try_cached_synthesis(&request, CancellationToken::new())
        .await
        .expect("successful native warmup must be cached");
    let mut bytes = 0;
    while let Some(event) = stream.next_event().await.unwrap() {
        match event {
            TtsEvent::Audio { pcm_s16le, .. } => bytes += pcm_s16le.len(),
            TtsEvent::AudioDone { .. } => break,
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(bytes > 0);
    assert_eq!(server.requests.lock().len(), 2);
    tokio::time::timeout(Duration::from_secs(2), server.task)
        .await
        .unwrap()
        .unwrap();
}

async fn multiple_audio_sessions(count: usize) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}/tts/websocket", listener.local_addr().unwrap());
    let requests: Requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = requests.clone();
    let task = tokio::spawn(async move {
        for _ in 0..count {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket =
                tokio_tungstenite::accept_hdr_async(stream, Handshake { refusal: None })
                    .await
                    .unwrap();
            while let Some(Ok(message)) = socket.next().await {
                let tokio_tungstenite::tungstenite::Message::Text(text) = message else {
                    break;
                };
                let value: Value = serde_json::from_str(&text).unwrap();
                recorded.lock().push(value.clone());
                let response = if value["continue"] == true {
                    json!({"type":"chunk","context_id":value["context_id"],"done":false,"status_code":206,"data":base64::engine::general_purpose::STANDARD.encode(vec![0;10000])})
                } else {
                    json!({"type":"done","context_id":value["context_id"],"done":true,"status_code":206})
                };
                socket
                    .send(tokio_tungstenite::tungstenite::Message::Text(
                        response.to_string().into(),
                    ))
                    .await
                    .unwrap();
            }
        }
    });
    Server {
        endpoint,
        requests,
        task,
    }
}

#[tokio::test]
async fn pcm_to_two_cartesia_providers_reaches_authorized_playback() {
    let tts_server = multiple_audio_sessions(2).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}/stt/websocket", listener.local_addr().unwrap());
    let asr_server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let mut frames = 0;
        let mut commits = 0;
        while let Some(Ok(message)) = socket.next().await {
            match message {
                tokio_tungstenite::tungstenite::Message::Binary(_) => frames += 1,
                tokio_tungstenite::tungstenite::Message::Text(text) if text == "finalize" => {
                    commits += 1;
                    for value in [
                        json!({"type":"transcript","request_id":"asr-connection","is_final":true,"text":"안녕하세요"}),
                        json!({"type":"flush_done","request_id":"asr-connection"}),
                    ] {
                        socket
                            .send(tokio_tungstenite::tungstenite::Message::Text(
                                value.to_string().into(),
                            ))
                            .await
                            .unwrap();
                    }
                }
                tokio_tungstenite::tungstenite::Message::Close(_) => break,
                _ => {}
            }
        }
        assert!(frames > 0 && commits == 1);
    });
    let asr_config = voice_live::config::AsrConfig {
        endpoint,
        provider: "cartesia".into(),
        model: "ink-whisper".into(),
        manual_commit: true,
        ..voice_live::config::AsrConfig::default()
    };
    let asr = voice_live::asr::cartesia::CartesiaStreamingAsr::with_api_key(
        &asr_config,
        "test-secret".into(),
    )
    .unwrap();
    let tts =
        CartesiaStreamingTts::with_api_key(&config(&tts_server.endpoint), "test-secret".into())
            .unwrap();
    let runtime_config = VoiceRuntimeConfig::from_yaml_str("speech:\n  mock_reply: Ready. Continue?\nasr:\n  manual_commit: true\ntts:\n  language: ko\n  voice: test-voice\nturn_control:\n  soft_silence_ms: 100\n  hard_silence_ms: 200\nobservability:\n  event_log: false").unwrap();
    let runtime = VoiceRuntime::build_with_providers(
        runtime_config,
        SpeechProviders {
            asr: Arc::new(asr),
            tts: Arc::new(tts),
        },
    )
    .await
    .unwrap();
    let (client_tx, mut client) = tokio::sync::mpsc::channel(256);
    let session = runtime.create_session(client_tx).await.unwrap();
    session
        .input_tx
        .send(voice_live::ClientInput::Control(
            voice_live::ClientControlMessage::SessionStart {
                input_sample_rate: 16000,
                channels: 1,
            },
        ))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    for sequence in 0..30 {
        let sample = if sequence < 10 { 12000 } else { 0 };
        session
            .input_tx
            .send(voice_live::ClientInput::Audio(
                voice_live::media::AudioFrame {
                    sequence,
                    pcm: vec![sample; 320],
                },
            ))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::timeout(Duration::from_secs(4), async {
        let mut current: Option<(SpeechId, u64)> = None;
        let mut sequence = 0;
        let mut samples = 0;
        let mut completed = 0;
        while let Some(message) = client.recv().await {
            match message {
                voice_live::ServerMessage::Json(value) if value["type"] == "speech_started" => {
                    current = Some((
                        serde_json::from_value(value["speech_id"].clone()).unwrap(),
                        value["speech_epoch"].as_u64().unwrap(),
                    ));
                    sequence = 0;
                    samples = 0;
                }
                voice_live::ServerMessage::Binary(bytes) => {
                    let (speech_id, speech_epoch) = current.expect("speech identity before PCM");
                    assert!(!bytes.is_empty());
                    sequence += 1;
                    samples += bytes.len() as u64 / 2;
                    session
                        .input_tx
                        .send(voice_live::ClientInput::Control(
                            voice_live::ClientControlMessage::PlaybackChunkCompleted {
                                speech_id,
                                speech_epoch,
                                sequence,
                                played_samples: samples,
                            },
                        ))
                        .await
                        .unwrap();
                }
                voice_live::ServerMessage::Json(value) if value["type"] == "speech_done" => {
                    assert!(samples > 0);
                    let (speech_id, speech_epoch) = current.take().unwrap();
                    session
                        .input_tx
                        .send(voice_live::ClientInput::Control(
                            voice_live::ClientControlMessage::PlaybackCompleted {
                                speech_id,
                                speech_epoch,
                            },
                        ))
                        .await
                        .unwrap();
                    completed += 1;
                    if completed == 2 {
                        return;
                    }
                }
                _ => {}
            }
        }
        panic!("missing Cartesia playback");
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while runtime.metrics().counter("voice_turn_resolved_total") != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        runtime.metrics().counter("voice_user_turn_committed_total"),
        1
    );
    assert_eq!(runtime.metrics().counter("voice_tts_failed_total"), 0);
    assert_eq!(tts_server.requests.lock().len(), 4);
    runtime.close_session(session.session_id).await;
    tokio::time::timeout(Duration::from_secs(2), asr_server)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), tts_server.task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn built_in_factory_selects_cartesia_and_validates_before_credentials() {
    let mut runtime_config = VoiceRuntimeConfig {
        tts: config("wss://api.cartesia.ai/tts/websocket"),
        ..VoiceRuntimeConfig::default()
    };
    runtime_config.tts.cartesia.api_version = "invalid".into();
    let error = VoiceRuntime::build(runtime_config)
        .await
        .err()
        .expect("factory rejects unsupported version");
    assert!(
        error
            .to_string()
            .contains("unsupported Cartesia API version")
    );
}

#[tokio::test]
async fn injected_runtime_routes_authorized_reply_through_cartesia() {
    let server = server(Reply::Audio).await;
    let tts = CartesiaStreamingTts::with_api_key(&config(&server.endpoint), "test-secret".into())
        .unwrap();
    let config = VoiceRuntimeConfig::from_yaml_str("speech:\n  mock_reply: Ready\nasr:\n  provider: inject\ntts:\n  language: ko\n  voice: test-voice\nturn_control:\n  soft_silence_ms: 100\n  hard_silence_ms: 200\nobservability:\n  event_log: false\ndev:\n  allow_text_injection: true").unwrap();
    let runtime = VoiceRuntime::build_with_providers(
        config,
        SpeechProviders {
            asr: Arc::new(voice_live::asr::fake::MockAsr::quiet()),
            tts: Arc::new(tts),
        },
    )
    .await
    .unwrap();
    let (client_tx, mut client) = tokio::sync::mpsc::channel(64);
    let session = runtime.create_session(client_tx).await.unwrap();
    assert!(runtime.inject_user_text(session.session_id, "안녕하세요"));
    tokio::time::timeout(Duration::from_secs(4), async {
        while let Some(message) = client.recv().await {
            if let voice_live::ServerMessage::Json(value) = message
                && value["type"] == "speech_done"
            {
                return;
            }
        }
        panic!("missing synthesized speech");
    })
    .await
    .unwrap();
    assert_eq!(server.requests.lock()[0]["model_id"], "sonic-3.6");
    runtime.close_session(session.session_id).await;
    tokio::time::timeout(Duration::from_secs(2), server.task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn injected_path_does_not_validate_unused_cartesia_settings() {
    let config = VoiceRuntimeConfig::from_yaml_str("tts:\n  provider: cartesia\n  cartesia:\n    api_version: ignored\n    max_buffer_delay_ms: 99999").unwrap();
    VoiceRuntime::build_with_providers(
        config,
        SpeechProviders {
            asr: Arc::new(voice_live::asr::fake::MockAsr::quiet()),
            tts: Arc::new(voice_live::tts::fake::FakeTts::new(24000, 60, 1)),
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn handshake_errors_are_typed_and_redacted() {
    for code in [
        "quota_exceeded",
        "plan_upgrade_required",
        "concurrency_limited",
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = tokio_tungstenite::accept_hdr_async(
                socket,
                Handshake {
                    refusal: Some(code),
                },
            )
            .await;
        });
        let provider =
            CartesiaStreamingTts::with_api_key(&config(&endpoint), "test-secret".into()).unwrap();
        let error = provider
            .open_text_stream(
                options(),
                TtsInputLimits {
                    max_input_bytes: 1024,
                    request_timeout: Duration::from_secs(1),
                },
                CancellationToken::new(),
            )
            .await
            .err()
            .unwrap();
        assert!(!error.to_string().contains("test-secret"));
        if code == "concurrency_limited" {
            assert!(matches!(error, TtsOpenError::Transient(_)));
        } else {
            assert!(matches!(error, TtsOpenError::Rejected(_)));
        }
        task.await.unwrap();
    }
}

#[tokio::test]
async fn legacy_synthesize_owns_native_workers_until_drop() {
    let server = server(Reply::Audio).await;
    let provider =
        CartesiaStreamingTts::with_api_key(&config(&server.endpoint), "test-secret".into())
            .unwrap();
    let mut stream = provider
        .synthesize(
            TtsRequest {
                speech_id: SpeechId::new(),
                speech_epoch: 1,
                text: "요청".into(),
                language: "Korean".into(),
                voice: "test-voice".into(),
                claim_class: ClaimClass::Phatic,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    while let Some(event) = stream.next_event().await.unwrap() {
        assert!(!matches!(event, TtsEvent::Error { .. }));
        if matches!(event, TtsEvent::AudioDone { .. }) {
            break;
        }
    }
    assert!(stream.next_event().await.unwrap().is_none());
    drop(stream);
    tokio::time::timeout(Duration::from_secs(2), server.task)
        .await
        .unwrap()
        .unwrap();
}
