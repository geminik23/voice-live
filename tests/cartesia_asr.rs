//! Offline Cartesia manual STT protocol and lifecycle tests, not vendor ordering proof.
#![cfg(feature = "cartesia")]
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::{
    Message,
    handshake::server::{Callback, ErrorResponse, Request, Response},
};
use tokio_util::sync::CancellationToken;
use voice_live::asr::cartesia::CartesiaStreamingAsr;
use voice_live::asr::{
    AsrAudioPush, AsrDuplexLimits, AsrDuplexSession, AsrEvent, AsrInputError, AsrOpenError,
    AsrSessionConfig, StreamingAsr,
};
use voice_live::config::AsrConfig;

type ServerSocket = WebSocketStream<TcpStream>;
const EXPECTED_QUERY: &[&str] = &[
    "model=ink-whisper",
    "encoding=pcm_s16le",
    "sample_rate=16000",
    "language=ko",
];

struct Handshake {
    expected_query: &'static [&'static str],
    rejection: Option<(u16, Option<&'static str>)>,
}
impl Callback for Handshake {
    fn on_request(self, request: &Request, response: Response) -> Result<Response, ErrorResponse> {
        assert_eq!(request.headers()["authorization"], "Bearer test-secret");
        assert_eq!(request.headers()["cartesia-version"], "2026-08-14");
        let query = request.uri().query().unwrap();
        for &field in self.expected_query {
            assert!(query.split('&').any(|value| value == field));
        }
        assert!(
            !query.contains("secret")
                && !query.contains("api_key")
                && !query.contains("cartesia_version")
        );
        match self.rejection {
            None => Ok(response),
            Some((status, code)) => Err(tokio_tungstenite::tungstenite::http::Response::builder()
                .status(status)
                .body(Some(
                    json!({"error_code":code,"message":"test-secret"}).to_string(),
                ))
                .unwrap()),
        }
    }
}
fn config(endpoint: &str) -> AsrConfig {
    AsrConfig {
        provider: "cartesia".into(),
        endpoint: endpoint.into(),
        model: "ink-whisper".into(),
        locale: "ko-KR".into(),
        manual_commit: true,
        api_key_env: "CARTESIA_API_KEY".into(),
        chunk_ms: 100,
        ..AsrConfig::default()
    }
}
fn session_config() -> AsrSessionConfig {
    AsrSessionConfig {
        sample_rate_hz: 16_000,
        locale: "ko-KR".into(),
        manual_commit: true,
    }
}
fn limits(samples: usize, commands: usize) -> AsrDuplexLimits {
    AsrDuplexLimits {
        sample_budget: samples,
        command_budget: commands,
        write_timeout: Duration::from_secs(1),
    }
}
async fn server<F, Fut>(handler: F) -> (AsrConfig, JoinHandle<()>)
where
    F: FnOnce(ServerSocket) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&format!(
        "ws://{}/stt/websocket",
        listener.local_addr().unwrap()
    ));
    let worker = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let socket = tokio_tungstenite::accept_hdr_async(
            stream,
            Handshake {
                expected_query: EXPECTED_QUERY,
                rejection: None,
            },
        )
        .await
        .unwrap();
        handler(socket).await;
    });
    (config, worker)
}
async fn open(config: &AsrConfig, limits: AsrDuplexLimits) -> AsrDuplexSession {
    CartesiaStreamingAsr::with_api_key(config, "test-secret".into())
        .unwrap()
        .open_duplex(session_config(), limits, CancellationToken::new())
        .await
        .unwrap()
}
async fn receive(socket: &mut ServerSocket) -> Message {
    tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
async fn json_send(socket: &mut ServerSocket, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
fn delta(text: &str, is_final: bool) -> Value {
    json!({"type":"transcript", "request_id":"connection", "text":text, "is_final":is_final})
}
fn ack() -> Value {
    json!({"type":"flush_done", "request_id":"connection"})
}
fn done() -> Value {
    json!({"type":"done", "request_id":"connection"})
}
async fn next(session: &mut AsrDuplexSession) -> Option<AsrEvent> {
    tokio::time::timeout(Duration::from_secs(3), session.events.next_event())
        .await
        .unwrap()
        .unwrap()
}
async fn cleanup(mut session: AsrDuplexSession, worker: JoinHandle<()>) {
    tokio::time::timeout(Duration::from_secs(2), session.control.close())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), worker)
        .await
        .unwrap()
        .unwrap();
}
async fn wait_close(socket: &mut ServerSocket) {
    while let Some(Ok(message)) = socket.next().await {
        if matches!(message, Message::Close(_)) {
            break;
        }
    }
}

#[tokio::test]
async fn english_session_locale_is_sent_on_wire_without_changing_defaults() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config(&format!(
        "ws://{}/stt/websocket",
        listener.local_addr().unwrap()
    ));
    cfg.locale = "en-US".into();
    let worker = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_hdr_async(
            stream,
            Handshake {
                expected_query: &[
                    "model=ink-whisper",
                    "encoding=pcm_s16le",
                    "sample_rate=16000",
                    "language=en",
                ],
                rejection: None,
            },
        )
        .await
        .unwrap();
        assert!(matches!(receive(&mut socket).await, Message::Binary(_)));
        json_send(&mut socket, delta("English speech", true)).await;
        assert_eq!(receive(&mut socket).await, Message::Text("finalize".into()));
        json_send(&mut socket, ack()).await;
        assert_eq!(receive(&mut socket).await, Message::Text("close".into()));
        json_send(&mut socket, done()).await;
        wait_close(&mut socket).await;
    });
    let provider = CartesiaStreamingAsr::with_api_key(&cfg, "test-secret".into()).unwrap();
    let mut session = provider
        .open_duplex(
            AsrSessionConfig {
                locale: "en-GB".into(),
                ..session_config()
            },
            limits(16000, 64),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    session.input.try_push_audio(vec![0; 320]).unwrap();
    assert!(
        matches!(next(&mut session).await,Some(AsrEvent::Partial {text,..}) if text == "English speech")
    );
    session.input.try_commit_utterance().unwrap();
    session.input.finish_input().unwrap();
    assert!(
        matches!(next(&mut session).await,Some(AsrEvent::Final {text,..}) if text == "English speech")
    );
    assert!(next(&mut session).await.is_none());
    // No built-in language: the session locale is always explicit.
    assert_eq!(AsrConfig::default().locale, "");
    cleanup(session, worker).await;
}

/// Ink Whisper returned an empty transcript for a short word sent as 20 ms messages and the right word for the same audio in 100 ms messages.
/// Frames are therefore batched to `chunk_ms`, and a commit first sends whatever audio is still held.
#[tokio::test]
async fn frames_are_sent_as_chunk_sized_messages_and_flushed_before_finalize() {
    let (config, worker) = server(|mut socket| async move {
        assert!(
            matches!(receive(&mut socket).await, Message::Binary(bytes) if bytes.len() == 1600 * 2)
        );
        assert!(
            matches!(receive(&mut socket).await, Message::Binary(bytes) if bytes.len() == 640 * 2)
        );
        assert_eq!(receive(&mut socket).await, Message::Text("finalize".into()));
        json_send(&mut socket, ack()).await;
        assert_eq!(receive(&mut socket).await, Message::Text("close".into()));
        json_send(&mut socket, done()).await;
        wait_close(&mut socket).await;
    })
    .await;
    let mut session = open(&config, limits(16000, 64)).await;
    for _ in 0..7 {
        session.input.push_audio(vec![0; 320]).await.unwrap();
    }
    session.input.commit_utterance().await.unwrap();
    assert!(matches!(
        next(&mut session).await,
        Some(AsrEvent::Final { .. })
    ));
    session.input.finish_input().unwrap();
    drop(session.input);
    assert!(session.events.next_event().await.unwrap().is_none());
    session.control.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn delta_accumulation_preserves_bytes_and_monotonic_identity() {
    let (config, worker) = server(|mut socket| async move {
        let Message::Binary(bytes) = receive(&mut socket).await else {
            panic!("raw PCM required")
        };
        assert_eq!(bytes.as_ref(), &[254, 255, 0, 0, 255, 127, 0, 128]);
        for (text, final_delta) in [
            ("토", true),
            ("요일 ", true),
            ("여섯 시", false),
            ("일곱 시", true),
        ] {
            json_send(&mut socket, delta(text, final_delta)).await;
        }
        assert_eq!(receive(&mut socket).await, Message::Text("finalize".into()));
        json_send(&mut socket, ack()).await;
        assert!(matches!(receive(&mut socket).await, Message::Binary(_)));
        json_send(&mut socket, delta("토요일 일곱 시", true)).await;
        assert_eq!(receive(&mut socket).await, Message::Text("finalize".into()));
        json_send(&mut socket, ack()).await;
        assert_eq!(receive(&mut socket).await, Message::Text("close".into()));
        json_send(&mut socket, done()).await;
        wait_close(&mut socket).await;
    })
    .await;
    let mut session = open(&config, limits(16000, 64)).await;
    assert!(matches!(
        session
            .input
            .try_push_audio(vec![-2, 0, 32767, -32768])
            .unwrap(),
        AsrAudioPush::Accepted
    ));
    for (index, expected) in ["토", "토요일 ", "토요일 여섯 시", "토요일 일곱 시"]
        .into_iter()
        .enumerate()
    {
        assert!(
            matches!(next(&mut session).await, Some(AsrEvent::Partial {utterance_id:0, revision, text}) if revision.0 == index as u64 + 1 && text == expected)
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(10), session.events.next_event())
            .await
            .is_err()
    );
    session.input.commit_utterance().await.unwrap();
    assert!(
        matches!(next(&mut session).await, Some(AsrEvent::Final {utterance_id:0,text}) if text == "토요일 일곱 시")
    );
    session.input.push_audio(vec![0; 320]).await.unwrap();
    assert!(
        matches!(next(&mut session).await, Some(AsrEvent::Partial {utterance_id:1,revision,text}) if revision.0 == 5 && text == "토요일 일곱 시")
    );
    session.input.commit_utterance().await.unwrap();
    session.input.finish_input().unwrap();
    session.input.finish_input().unwrap();
    assert!(matches!(
        session.input.try_commit_utterance(),
        Err(AsrInputError::Closed)
    ));
    assert!(
        matches!(next(&mut session).await, Some(AsrEvent::Final {utterance_id:1,text}) if text == "토요일 일곱 시")
    );
    assert!(next(&mut session).await.is_none());
    cleanup(session, worker).await;
}

#[tokio::test]
async fn writer_pipelines_pcm_and_multiple_flushes_without_ack_barrier() {
    let (config, worker) = server(|mut socket| async move {
        for _ in 0..2 {
            assert!(matches!(receive(&mut socket).await, Message::Binary(_)));
            assert_eq!(receive(&mut socket).await, Message::Text("finalize".into()));
        }
        for text in ["첫 발화", "두 번째"] {
            json_send(&mut socket, delta(text, true)).await;
            json_send(&mut socket, ack()).await;
        }
        assert_eq!(receive(&mut socket).await, Message::Text("close".into()));
        json_send(&mut socket, done()).await;
        wait_close(&mut socket).await;
    })
    .await;
    let mut session = open(&config, limits(16000, 64)).await;
    for _ in 0..2 {
        session.input.try_push_audio(vec![0; 320]).unwrap();
        session.input.try_commit_utterance().unwrap();
    }
    session.input.finish_input().unwrap();
    for (id, text) in [(0, "첫 발화"), (1, "두 번째")] {
        assert!(
            matches!(next(&mut session).await,Some(AsrEvent::Partial {utterance_id,text:actual,..}) if utterance_id == id && actual == text)
        );
        assert!(
            matches!(next(&mut session).await,Some(AsrEvent::Final {utterance_id,text:actual}) if utterance_id == id && actual == text)
        );
    }
    assert!(next(&mut session).await.is_none());
    cleanup(session, worker).await;
}

#[tokio::test]
async fn pending_markers_keep_command_budget_until_ack_and_finish_is_reserved() {
    let observed = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let seen = observed.clone();
    let signal = release.clone();
    let (config, worker) = server(move |mut socket| async move {
        for _ in 0..2 {
            assert_eq!(receive(&mut socket).await, Message::Text("finalize".into()));
        }
        seen.notify_one();
        signal.notified().await;
        json_send(&mut socket, ack()).await;
        assert_eq!(receive(&mut socket).await, Message::Text("finalize".into()));
        json_send(&mut socket, ack()).await;
        json_send(&mut socket, ack()).await;
        assert_eq!(receive(&mut socket).await, Message::Text("close".into()));
        json_send(&mut socket, done()).await;
        wait_close(&mut socket).await;
    })
    .await;
    let mut session = open(&config, limits(16000, 2)).await;
    session.input.try_commit_utterance().unwrap();
    session.input.try_commit_utterance().unwrap();
    observed.notified().await;
    assert!(matches!(
        session.input.try_commit_utterance(),
        Err(AsrInputError::Full)
    ));
    release.notify_one();
    assert!(
        matches!(next(&mut session).await,Some(AsrEvent::Final {utterance_id:0,text}) if text.is_empty())
    );
    session.input.try_commit_utterance().unwrap();
    session.input.finish_input().unwrap();
    for id in [1, 2] {
        assert!(
            matches!(next(&mut session).await,Some(AsrEvent::Final {utterance_id,text}) if utterance_id == id && text.is_empty())
        );
    }
    assert!(next(&mut session).await.is_none());
    cleanup(session, worker).await;
}

#[tokio::test]
async fn pcm_permit_covers_writer_pacing_and_conversion_lifetime() {
    let observed = Arc::new(Notify::new());
    let signal = observed.clone();
    let (config, worker) = server(move |mut socket| async move {
        assert!(matches!(receive(&mut socket).await, Message::Binary(_)));
        signal.notify_one();
        assert!(matches!(receive(&mut socket).await, Message::Binary(_)));
        assert_eq!(receive(&mut socket).await, Message::Text("close".into()));
        json_send(&mut socket, done()).await;
        wait_close(&mut socket).await;
    })
    .await;
    let mut session = open(&config, limits(3200, 64)).await;
    session.input.try_push_audio(vec![0; 3200]).unwrap();
    observed.notified().await;
    assert!(
        matches!(session.input.try_push_audio(vec![1]),Ok(AsrAudioPush::Full(pcm)) if pcm == vec![1])
    );
    assert!(
        matches!(session.input.push_audio(vec![0;3201]).await,Ok(AsrAudioPush::Full(pcm)) if pcm.len() == 3201)
    );
    session.input.finish_input().unwrap();
    assert!(next(&mut session).await.is_none());
    cleanup(session, worker).await;
}

#[tokio::test]
async fn graceful_done_emits_only_confirmed_text_and_survives_input_drop() {
    let (config, worker) = server(|mut socket| async move {
        assert!(matches!(receive(&mut socket).await, Message::Binary(_)));
        json_send(&mut socket, delta("확정", true)).await;
        json_send(&mut socket, delta(" 추정", false)).await;
        assert_eq!(receive(&mut socket).await, Message::Text("close".into()));
        json_send(&mut socket, done()).await;
        wait_close(&mut socket).await;
    })
    .await;
    let mut session = open(&config, limits(16000, 64)).await;
    session.input.push_audio(vec![0; 320]).await.unwrap();
    assert!(matches!(
        next(&mut session).await,
        Some(AsrEvent::Partial { .. })
    ));
    assert!(
        matches!(next(&mut session).await,Some(AsrEvent::Partial {text,..}) if text == "확정 추정")
    );
    session.input.finish_input().unwrap();
    drop(session.input);
    assert!(
        matches!(tokio::time::timeout(Duration::from_secs(2),session.events.next_event()).await.unwrap().unwrap(),Some(AsrEvent::Final {text,..}) if text == "확정")
    );
    assert!(session.events.next_event().await.unwrap().is_none());
    session.control.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn continuous_silence_and_repeated_delayed_flushes_do_not_accumulate_backlog() {
    let (config,worker) = server(|mut socket| async move {
        let mut pending = std::collections::VecDeque::new();
        let mut samples = 0; let mut commits = 0;
        let mut first = None; let mut last = tokio::time::Instant::now();
        let mut timer = tokio::time::interval(Duration::from_millis(5));
        loop {
            tokio::select! {
                _ = timer.tick() => {
                    while pending.front().is_some_and(|deadline| *deadline <= tokio::time::Instant::now()) {
                        pending.pop_front(); json_send(&mut socket,ack()).await;
                    }
                }
                message = socket.next() => match message.unwrap().unwrap() {
                    Message::Binary(pcm) => { let now = tokio::time::Instant::now(); first.get_or_insert(now); last = now; samples += pcm.len()/2; }
                    Message::Text(text) if text == "finalize" => { commits += 1; pending.push_back(tokio::time::Instant::now()+Duration::from_millis(200)); }
                    Message::Text(text) if text == "close" => {
                        assert!(pending.is_empty()); assert_eq!(commits,12); assert_eq!(samples,19200);
                        assert!(last.duration_since(first.unwrap()) >= Duration::from_millis(1000), "audio must be paced, not burst");
                        json_send(&mut socket,done()).await; wait_close(&mut socket).await; return;
                    }
                    other => panic!("unexpected {other:?}"),
                }
            }
        }
    }).await;
    let mut session = open(&config, limits(3200, 64)).await;
    for frame in 0..60 {
        assert!(matches!(
            session.input.try_push_audio(vec![0; 320]),
            Ok(AsrAudioPush::Accepted)
        ));
        if frame % 5 == 4 {
            session.input.try_commit_utterance().unwrap();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    session.input.finish_input().unwrap();
    for id in 0..12 {
        assert!(
            matches!(next(&mut session).await,Some(AsrEvent::Final {utterance_id,..}) if utterance_id == id)
        );
    }
    assert!(next(&mut session).await.is_none());
    cleanup(session, worker).await;
}

#[tokio::test]
async fn finalize_deadline_is_absolute_despite_ping_traffic() {
    let (mut config,worker) = server(|mut socket| async move {
        assert_eq!(receive(&mut socket).await,Message::Text("finalize".into()));
        let mut timer = tokio::time::interval(Duration::from_millis(10));
        loop {
            tokio::select! {
                _ = timer.tick() => { if socket.send(Message::Ping(vec![1].into())).await.is_err() { return; } }
                message = socket.next() => { if !matches!(message, Some(Ok(Message::Pong(_)))) { return; } }
            }
        }
    }).await;
    config.cartesia.finalize_timeout_ms = 80;
    let mut session = open(&config, limits(16000, 64)).await;
    session.input.try_commit_utterance().unwrap();
    assert!(
        matches!(next(&mut session).await,Some(AsrEvent::Error {recoverable:true,message}) if message.contains("finalize timed out"))
    );
    assert!(next(&mut session).await.is_none());
    cleanup(session, worker).await;
}

#[tokio::test]
async fn finish_deadline_covers_pcm_drain_not_just_close_ack() {
    let (mut config, worker) = server(|mut socket| async move {
        while let Some(Ok(message)) = socket.next().await {
            match message {
                Message::Binary(_) => {}
                Message::Close(_) => return,
                other => panic!("finish must expire before draining 1s audio: {other:?}"),
            }
        }
    })
    .await;
    config.cartesia.finish_timeout_ms = 60;
    let mut session = open(&config, limits(16000, 64)).await;
    session.input.try_push_audio(vec![0; 16000]).unwrap();
    session.input.finish_input().unwrap();
    assert!(
        matches!(next(&mut session).await,Some(AsrEvent::Error {message,..}) if message.contains("finish timed out"))
    );
    cleanup(session, worker).await;
}

#[tokio::test]
async fn finish_deadline_retains_error_with_full_unpolled_output() {
    let (mut config, worker) = server(|mut socket| async move {
        assert!(matches!(receive(&mut socket).await, Message::Binary(_)));
        for n in 0..40 {
            json_send(&mut socket, delta(&format!("가설 {n}"), false)).await;
        }
        assert_eq!(receive(&mut socket).await, Message::Text("close".into()));
        wait_close(&mut socket).await;
    })
    .await;
    config.cartesia.finish_timeout_ms = 80;
    let mut session = open(&config, limits(16000, 64)).await;
    session.input.try_push_audio(vec![0; 320]).unwrap();
    session.input.finish_input().unwrap();
    tokio::time::sleep(Duration::from_millis(160)).await;
    assert!(
        matches!(next(&mut session).await,Some(AsrEvent::Error {message,..}) if message.contains("finish timed out"))
    );
    assert!(next(&mut session).await.is_none());
    cleanup(session, worker).await;
}

#[tokio::test]
async fn finish_deadline_includes_pending_flush_drain() {
    let (mut config, worker) = server(|mut socket| async move {
        assert_eq!(receive(&mut socket).await, Message::Text("finalize".into()));
        wait_close(&mut socket).await;
    })
    .await;
    config.cartesia.finish_timeout_ms = 60;
    let mut session = open(&config, limits(16000, 64)).await;
    session.input.try_commit_utterance().unwrap();
    session.input.finish_input().unwrap();
    assert!(
        matches!(next(&mut session).await,Some(AsrEvent::Error {message,..}) if message.contains("finish timed out"))
    );
    cleanup(session, worker).await;
}

#[tokio::test]
async fn malformed_known_events_and_unsolicited_boundaries_fail_closed() {
    for response in [
        ack(),
        done(),
        json!({"type":"transcript","request_id":"connection","text":"missing flag"}),
        json!({"type":"transcript","request_id":"connection","is_final":true}),
        json!({"type":"transcript","is_final":true,"text":"missing id"}),
    ] {
        let (config, worker) = server(move |mut socket| async move {
            assert!(matches!(receive(&mut socket).await, Message::Binary(_)));
            json_send(&mut socket, response).await;
            wait_close(&mut socket).await;
        })
        .await;
        let mut session = open(&config, limits(16000, 64)).await;
        session.input.try_push_audio(vec![0; 320]).unwrap();
        assert!(matches!(
            next(&mut session).await,
            Some(AsrEvent::Error {
                recoverable: false,
                ..
            })
        ));
        cleanup(session, worker).await;
    }
}

#[tokio::test]
async fn transcript_budget_counts_confirmed_prefix_plus_mutable_tail_in_utf8() {
    let proceed = Arc::new(Notify::new());
    let signal = proceed.clone();
    let (mut config, worker) = server(move |mut socket| async move {
        assert!(matches!(receive(&mut socket).await, Message::Binary(_)));
        json_send(&mut socket, delta("가", true)).await;
        signal.notified().await;
        json_send(&mut socket, delta("나", false)).await;
        wait_close(&mut socket).await;
    })
    .await;
    config.cartesia.max_transcript_bytes = 3;
    let mut session = open(&config, limits(16000, 64)).await;
    session.input.try_push_audio(vec![0; 320]).unwrap();
    assert!(matches!(next(&mut session).await,Some(AsrEvent::Partial {text,..}) if text == "가"));
    proceed.notify_one();
    assert!(
        matches!(next(&mut session).await,Some(AsrEvent::Error {message,..}) if message.contains("byte budget"))
    );
    cleanup(session, worker).await;
}

#[tokio::test]
async fn repeated_snapshot_does_not_fabricate_revisions_and_id_change_is_rejected() {
    let proceed = Arc::new(Notify::new());
    let signal = proceed.clone();
    let (config, worker) = server(move |mut socket| async move {
        assert!(matches!(receive(&mut socket).await, Message::Binary(_)));
        json_send(&mut socket, delta("같음", false)).await;
        json_send(&mut socket, delta("같음", false)).await;
        signal.notified().await;
        let mut changed = delta("변경", true);
        changed["request_id"] = json!("other");
        json_send(&mut socket, changed).await;
        wait_close(&mut socket).await;
    })
    .await;
    let mut session = open(&config, limits(16000, 64)).await;
    session.input.try_push_audio(vec![0; 320]).unwrap();
    assert!(
        matches!(next(&mut session).await,Some(AsrEvent::Partial {revision,..}) if revision.0 == 1)
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), session.events.next_event())
            .await
            .is_err()
    );
    proceed.notify_one();
    assert!(
        matches!(next(&mut session).await,Some(AsrEvent::Error {message,..}) if message.contains("identity changed"))
    );
    cleanup(session, worker).await;
}

#[tokio::test]
async fn contextless_provider_errors_preserve_retry_classification_without_leaking_text() {
    for (code, recoverable) in [
        (Some("quota_exceeded"), false),
        (Some("plan_upgrade_required"), false),
        (Some("concurrency_limited"), true),
        (None, true),
    ] {
        let (config,worker) = server(move |mut socket| async move {
            json_send(&mut socket,json!({"type":"error","status_code":429,"error_code":code,"message":"test-secret transcript"})).await;
            wait_close(&mut socket).await;
        }).await;
        let mut session = open(&config, limits(16000, 64)).await;
        let Some(AsrEvent::Error {
            recoverable: actual,
            message,
        }) = next(&mut session).await
        else {
            panic!("error expected")
        };
        assert_eq!(actual, recoverable);
        assert!(!message.contains("test-secret") && !message.contains("transcript"));
        cleanup(session, worker).await;
    }
}

#[tokio::test]
async fn acknowledged_final_survives_a_later_failure_before_output_polling() {
    let (config, worker) = server(|mut socket| async move {
        assert!(matches!(receive(&mut socket).await, Message::Binary(_)));
        assert_eq!(receive(&mut socket).await, Message::Text("finalize".into()));
        json_send(&mut socket, delta("확정된 발화", true)).await;
        json_send(&mut socket, ack()).await;
        json_send(&mut socket, delta("unconfirmed successor", false)).await;
    })
    .await;
    let mut session = open(&config, limits(16000, 64)).await;
    session.input.try_push_audio(vec![0; 320]).unwrap();
    session.input.try_commit_utterance().unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if matches!(
                session.input.try_push_audio(Vec::new()),
                Err(AsrInputError::Closed)
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        matches!(next(&mut session).await, Some(AsrEvent::Final { utterance_id:0,text }) if text == "확정된 발화")
    );
    assert!(matches!(
        next(&mut session).await,
        Some(AsrEvent::Error {
            recoverable: true,
            ..
        })
    ));
    assert!(next(&mut session).await.is_none());
    cleanup(session, worker).await;
}

#[tokio::test]
async fn pings_are_serviced_while_a_multi_chunk_pcm_command_is_paced() {
    let (config, worker) = server(|mut socket| async move {
        let Message::Binary(first) = receive(&mut socket).await else {
            panic!("PCM expected")
        };
        let mut bytes = first.len();
        let mut pongs = 0;
        for n in 0..10 {
            socket.send(Message::Ping(vec![n].into())).await.unwrap();
        }
        loop {
            match receive(&mut socket).await {
                Message::Binary(pcm) => bytes += pcm.len(),
                Message::Pong(_) => pongs += 1,
                Message::Text(text) if text == "finalize" => {
                    assert_eq!(bytes, 16000);
                    assert!(pongs >= 10);
                    json_send(&mut socket, delta("발화", true)).await;
                    json_send(&mut socket, ack()).await;
                }
                Message::Text(text) if text == "close" => {
                    json_send(&mut socket, done()).await;
                    wait_close(&mut socket).await;
                    return;
                }
                other => panic!("unexpected {other:?}"),
            }
        }
    })
    .await;
    let mut session = open(&config, limits(16000, 64)).await;
    session.input.try_push_audio(vec![0; 8000]).unwrap();
    session.input.try_commit_utterance().unwrap();
    session.input.finish_input().unwrap();
    assert!(matches!(
        next(&mut session).await,
        Some(AsrEvent::Partial { .. })
    ));
    assert!(matches!(next(&mut session).await,Some(AsrEvent::Final {text,..}) if text == "발화"));
    assert!(next(&mut session).await.is_none());
    cleanup(session, worker).await;
}

#[tokio::test]
async fn eof_without_done_is_failure_and_never_promotes_unconfirmed_tail() {
    let (config, worker) = server(|mut socket| async move {
        assert!(matches!(receive(&mut socket).await, Message::Binary(_)));
        json_send(&mut socket, delta("확정", true)).await;
        json_send(&mut socket, delta(" 추정", false)).await;
        assert_eq!(receive(&mut socket).await, Message::Text("close".into()));
    })
    .await;
    let mut session = open(&config, limits(16000, 64)).await;
    session.input.try_push_audio(vec![0; 320]).unwrap();
    for _ in 0..2 {
        assert!(matches!(
            next(&mut session).await,
            Some(AsrEvent::Partial { .. })
        ));
    }
    session.input.finish_input().unwrap();
    assert!(matches!(
        next(&mut session).await,
        Some(AsrEvent::Error { .. })
    ));
    assert!(next(&mut session).await.is_none());
    cleanup(session, worker).await;
}

#[tokio::test]
async fn cancelled_admission_does_not_consume_command_capacity() {
    let observed = Arc::new(Notify::new());
    let signal = observed.clone();
    let release = Arc::new(Notify::new());
    let ack_signal = release.clone();
    let (config, worker) = server(move |mut socket| async move {
        assert_eq!(receive(&mut socket).await, Message::Text("finalize".into()));
        signal.notify_one();
        ack_signal.notified().await;
        json_send(&mut socket, ack()).await;
        assert_eq!(receive(&mut socket).await, Message::Text("close".into()));
        json_send(&mut socket, done()).await;
        wait_close(&mut socket).await;
    })
    .await;
    let mut session = open(&config, limits(16000, 1)).await;
    session.input.try_commit_utterance().unwrap();
    observed.notified().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), session.input.commit_utterance())
            .await
            .is_err()
    );
    assert!(matches!(
        session.input.try_push_audio(vec![1]),
        Ok(AsrAudioPush::Full(_))
    ));
    release.notify_one();
    assert!(matches!(
        next(&mut session).await,
        Some(AsrEvent::Final { .. })
    ));
    session.input.finish_input().unwrap();
    assert!(next(&mut session).await.is_none());
    cleanup(session, worker).await;
}

#[tokio::test]
async fn drop_and_cancel_end_socket_ownership_without_polling_output() {
    for kind in 0..4 {
        let (config, worker) = server(|mut socket| async move {
            wait_close(&mut socket).await;
        })
        .await;
        let mut session = open(&config, limits(16000, 64)).await;
        match kind {
            0 => {
                drop(session.input);
                tokio::time::timeout(Duration::from_secs(2), worker)
                    .await
                    .unwrap()
                    .unwrap();
                session.control.close().await.unwrap();
            }
            1 => {
                drop(session.events);
                tokio::time::timeout(Duration::from_secs(2), worker)
                    .await
                    .unwrap()
                    .unwrap();
                session.control.close().await.unwrap();
            }
            2 => {
                session.control.cancel();
                tokio::time::timeout(Duration::from_secs(2), worker)
                    .await
                    .unwrap()
                    .unwrap();
                session.control.close().await.unwrap();
            }
            _ => {
                drop(session.control);
                tokio::time::timeout(Duration::from_secs(2), worker)
                    .await
                    .unwrap()
                    .unwrap();
            }
        }
    }
}

#[tokio::test]
async fn legacy_facade_and_fresh_connections_use_native_manual_stream() {
    let (config, worker) = server(|mut socket| async move {
        assert!(matches!(receive(&mut socket).await, Message::Binary(_)));
        assert_eq!(receive(&mut socket).await, Message::Text("finalize".into()));
        json_send(&mut socket, delta("발화", true)).await;
        json_send(&mut socket, ack()).await;
        wait_close(&mut socket).await;
    })
    .await;
    let provider = CartesiaStreamingAsr::with_api_key(&config, "test-secret".into()).unwrap();
    let mut legacy = provider.open(session_config()).await.unwrap();
    legacy.push_audio(&[0; 320]).await.unwrap();
    legacy.commit_audio().await.unwrap();
    assert!(matches!(
        legacy.next_event().await.unwrap(),
        Some(AsrEvent::Partial {
            utterance_id: 0,
            ..
        })
    ));
    assert!(matches!(
        legacy.next_event().await.unwrap(),
        Some(AsrEvent::Final {
            utterance_id: 0,
            ..
        })
    ));
    legacy.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn finish_without_pcm_is_graceful_and_empty() {
    let (config, worker) = server(|mut socket| async move {
        assert_eq!(receive(&mut socket).await, Message::Text("close".into()));
        json_send(&mut socket, done()).await;
        wait_close(&mut socket).await;
    })
    .await;
    let mut session = open(&config, limits(16000, 64)).await;
    session.input.finish_input().unwrap();
    session.input.finish_input().unwrap();
    assert!(next(&mut session).await.is_none());
    cleanup(session, worker).await;
}

#[tokio::test]
async fn input_drop_after_finish_keeps_owner_deadline_and_terminal() {
    let (mut config, worker) = server(|mut socket| async move {
        assert_eq!(receive(&mut socket).await, Message::Text("close".into()));
        wait_close(&mut socket).await;
    })
    .await;
    config.cartesia.finish_timeout_ms = 60;
    let mut session = open(&config, limits(16000, 64)).await;
    session.input.finish_input().unwrap();
    drop(session.input);
    assert!(
        matches!(tokio::time::timeout(Duration::from_secs(2),session.events.next_event()).await.unwrap().unwrap(),Some(AsrEvent::Error {message,..}) if message.contains("finish timed out"))
    );
    session.control.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn handshake_errors_are_classified_and_redacted() {
    for (status, code, retry) in [
        (400, Some("model_not_found"), false),
        (401, None, false),
        (429, Some("quota_exceeded"), false),
        (429, Some("concurrency_limited"), true),
        (503, None, true),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = config(&format!("ws://{}/stt", listener.local_addr().unwrap()));
        let worker = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = tokio_tungstenite::accept_hdr_async(
                socket,
                Handshake {
                    expected_query: EXPECTED_QUERY,
                    rejection: Some((status, code)),
                },
            )
            .await;
        });
        let provider = CartesiaStreamingAsr::with_api_key(&config, "test-secret".into()).unwrap();
        let error = provider
            .open_duplex(
                session_config(),
                limits(16000, 64),
                CancellationToken::new(),
            )
            .await
            .err()
            .unwrap();
        assert_eq!(error.retryable(), retry);
        assert!(!error.to_string().contains("test-secret"));
        worker.await.unwrap();
    }
}

#[tokio::test]
async fn built_in_and_injected_factories_keep_validation_boundaries() {
    use voice_live::{SpeechProviders, VoiceRuntime, VoiceRuntimeConfig};
    let mut config = VoiceRuntimeConfig {
        asr: config("wss://api.cartesia.ai/stt/websocket"),
        ..VoiceRuntimeConfig::default()
    };
    config.asr.cartesia.api_version = "unsupported".into();
    assert!(
        VoiceRuntime::build(config)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("unsupported Cartesia ASR API version")
    );
    let config = VoiceRuntimeConfig::from_yaml_str("asr:\n  provider: cartesia\n  cartesia:\n    api_version: unused\n    finalize_timeout_ms: 0\n    finish_timeout_ms: 0\n    max_transcript_bytes: 0").unwrap();
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

#[test]
fn factory_validation_rejects_unsupported_or_unsafe_settings_without_secrets() {
    let base = config("wss://api.cartesia.ai/stt/websocket");
    assert_eq!(base.cartesia.finalize_timeout_ms, 2000);
    assert_eq!(base.cartesia.finish_timeout_ms, 5000);
    assert_eq!(base.cartesia.max_transcript_bytes, 65536);
    for endpoint in [
        "ws://example.com/stt",
        "wss://user:secret@api.cartesia.ai/stt",
        "wss://api.cartesia.ai/stt?access_token=secret",
        "wss://api.cartesia.ai/stt?model=other",
    ] {
        let result = CartesiaStreamingAsr::with_api_key(
            &AsrConfig {
                endpoint: endpoint.into(),
                ..base.clone()
            },
            "secret".into(),
        );
        assert!(result.is_err());
        assert!(!result.err().unwrap().to_string().contains("secret"));
    }
    for invalid in [
        AsrConfig {
            manual_commit: false,
            ..base.clone()
        },
        AsrConfig {
            locale: "fr&language=en".into(),
            ..base.clone()
        },
        AsrConfig {
            model: "ink-2".into(),
            ..base.clone()
        },
        AsrConfig {
            chunk_ms: 0,
            ..base.clone()
        },
    ] {
        assert!(CartesiaStreamingAsr::with_api_key(&invalid, "secret".into()).is_err());
    }
}

#[tokio::test]
async fn open_rejects_incompatible_session_options_before_network_io() {
    let provider = CartesiaStreamingAsr::with_api_key(
        &config("wss://api.cartesia.ai/stt/websocket"),
        "secret".into(),
    )
    .unwrap();
    for session in [
        AsrSessionConfig {
            sample_rate_hz: 48000,
            ..session_config()
        },
        AsrSessionConfig {
            manual_commit: false,
            ..session_config()
        },
        AsrSessionConfig {
            locale: "fr--CA".into(),
            ..session_config()
        },
    ] {
        assert!(matches!(
            provider
                .open_duplex(session, limits(16000, 64), CancellationToken::new())
                .await,
            Err(AsrOpenError::Rejected(_))
        ));
    }
}
