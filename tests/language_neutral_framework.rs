#![cfg(feature = "framework")]
use ai_agents::agent::AgentBuilder;
use ai_agents::hooks::AgentHooks;
use ai_agents::memory::{InMemoryStore, Memory};
use ai_agents::tools::{
    Tool, ToolExecutionContext, ToolOperationKind, ToolResult, ToolSafetyMetadata,
};
use ai_agents_llm::mock::MockLLMProvider;
use async_trait::async_trait;
use serde_json::{Value, json};
use std::sync::Arc;
use voice_live::agent::framework::{AiAgentsCognitiveAgent, VoiceAgentHooks};
use voice_live::agent::{
    AgentTurnRequest, CognitiveAgent, CognitiveEvent, TurnContext, TurnResolution,
};

struct Create;
#[async_trait]
impl Tool for Create {
    fn id(&self) -> &str {
        "create"
    }
    fn name(&self) -> &str {
        "Create"
    }
    fn description(&self) -> &str {
        "Offline outcome tool"
    }
    fn input_schema(&self) -> Value {
        json!({"type":"object","properties":{"target":{"type":"string"}},"required":["target"]})
    }
    fn safety_metadata(&self) -> ToolSafetyMetadata {
        ToolSafetyMetadata::read_only(ToolOperationKind::Read)
    }
    async fn execute(&self, args: Value, ctx: ToolExecutionContext) -> ToolResult {
        assert!(
            ctx.actor
                .sender_agent_id
                .as_deref()
                .unwrap()
                .starts_with("voice-turn-")
        );
        ToolResult::ok(json!({"status":"committed","target":args["target"]}).to_string())
    }
}

#[tokio::test]
async fn published_framework_proof_origin_and_rendered_memory_keep_original_final_identity() {
    proof_origin_and_rendered_memory(false).await;
}

/// With SDK tool events on (as in the demo spec), a bound tool still reports exactly one start and one completion, and the completion carries the output.
#[tokio::test]
async fn bound_tool_events_are_not_duplicated_when_sdk_tool_events_are_on() {
    proof_origin_and_rendered_memory(true).await;
}

async fn proof_origin_and_rendered_memory(include_tool_events: bool) {
    let proposal = r#"{"text":"Continue?"}"#;
    let mut mock = MockLLMProvider::new("offline");
    mock.set_responses(
        vec![
            r#"{"tool":"create","arguments":{"target":"A"}}"#.into(),
            proposal.into(),
        ],
        false,
    );
    let hooks = VoiceAgentHooks::new();
    let memory: Arc<dyn Memory> = Arc::new(InMemoryStore::new(40));
    let runtime = AgentBuilder::from_yaml(&format!(
        "name: OfflineProof\nsystem_prompt: Offline proof test\ntools: [create]\nstreaming: {{enabled: true, include_tool_events: {include_tool_events}}}"
    ))
    .unwrap()
    .system_prompt("Use create, then return a proposal.")
    .llm(Arc::new(mock))
    .memory(memory.clone())
    .hooks(hooks.clone() as Arc<dyn AgentHooks>)
    .tool(hooks.bind_tool(Arc::new(Create)))
    .build()
    .unwrap();
    let agent = AiAgentsCognitiveAgent::with_turn_memory(Arc::new(runtime), hooks, memory.clone());
    let task_id = voice_live::TaskId::new();
    let mut stream = agent
        .start_turn(AgentTurnRequest {
            task_id,
            thought_epoch: 9,
            input: "request".into(),
            context: TurnContext::default(),
            speculative: false,
            dependencies: vec![],
        })
        .await
        .unwrap();
    let mut records = Vec::new();
    let mut started = 0;
    let mut completed = 0;
    loop {
        match stream.next_event().await.unwrap() {
            Some(CognitiveEvent::InvocationProof {
                task_id: origin,
                thought_epoch,
                proof,
            }) => {
                assert_eq!(origin, task_id);
                assert_eq!(thought_epoch, 9);
                assert_eq!(proof.arguments, json!({"target":"A"}));
                assert!(proof.usable());
                records.push(proof);
            }
            Some(CognitiveEvent::ToolStarted { tool, .. }) => {
                assert_eq!(tool, "create");
                started += 1;
            }
            Some(CognitiveEvent::ToolCompleted { tool, output, .. }) => {
                assert_eq!(tool, "create");
                assert!(output.contains("committed"), "{output}");
                completed += 1;
            }
            Some(CognitiveEvent::Final { text }) => {
                assert_eq!(text, proposal);
                break;
            }
            Some(CognitiveEvent::Failed { message }) => panic!("{message}"),
            Some(_) => {}
            None => panic!("missing final"),
        }
    }
    assert_eq!(
        records.len(),
        1,
        "memory: {:?}",
        memory.get_messages(None).await.unwrap()
    );
    assert_eq!(started, 1);
    assert_eq!(completed, 1);
    drop(stream);
    agent.resolve_turn(
        task_id,
        TurnResolution::Audible {
            final_text: proposal.into(),
            heard: "Continue?".into(),
        },
    );
    for _ in 0..100 {
        tokio::task::yield_now().await;
        let messages = memory.get_messages(None).await.unwrap();
        if messages.iter().any(|m| m.content == "Continue?") {
            assert!(!messages.iter().any(|m| m.content == proposal));
            return;
        }
    }
    panic!("rendered text did not replace the exact original proposal");
}

#[tokio::test]
async fn semantic_provider_uses_only_current_heard_context_per_request() {
    use voice_live::interaction::brain::InteractionBrain;
    use voice_live::interaction::{InteractionSnapshot, UserSpeechState};

    let mut mock = MockLLMProvider::new("semantic");
    mock.set_responses(vec![r#"{"state":"answer","complete":true}"#.into()], true);
    let brain = voice_live::agent::framework::RuntimeSemanticBrain::new(
        Arc::new(mock.clone()),
        "Interpret the current speech.".into(),
    );
    let first = InteractionSnapshot {
        turn_candidate: "first partial".into(),
        heard_assistant: "Which day?".into(),
        ..InteractionSnapshot::default()
    };
    assert_eq!(
        brain.assess(first).await.unwrap().state,
        UserSpeechState::Answer
    );
    let second = InteractionSnapshot {
        turn_candidate: "revised answer".into(),
        heard_assistant: "How many people?".into(),
        ..InteractionSnapshot::default()
    };
    assert_eq!(
        brain.assess(second).await.unwrap().state,
        UserSpeechState::Answer
    );
    let calls = mock.call_history();
    assert_eq!(calls.len(), 2);
    let second_messages = &calls[1].messages;
    assert_eq!(second_messages.len(), 2);
    let content = &second_messages[1].content;
    assert!(content.contains("revised answer"));
    assert!(content.contains("How many people?"));
    assert!(!content.contains("first partial"));
    assert!(!content.contains("Which day?"));
}

#[tokio::test(start_paused = true)]
async fn semantic_host_factory_runs_with_mock_task_agent_per_session() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use voice_live::interaction::brain::InteractionBrain;

    let mut mock = MockLLMProvider::new("semantic");
    mock.set_responses(vec![r#"{"state":"complete","complete":true}"#.into()], true);
    let factories = Arc::new(AtomicUsize::new(0));
    let count = factories.clone();
    let provider = mock.clone();
    let policies = voice_live::RuntimePolicies::default().with_interaction_brain(move || {
        count.fetch_add(1, Ordering::SeqCst);
        Arc::new(voice_live::agent::framework::RuntimeSemanticBrain::new(
            Arc::new(provider.clone()),
            "Interpret the current speech.".into(),
        )) as Arc<dyn InteractionBrain>
    });
    let config = voice_live::VoiceRuntimeConfig::from_yaml_str(
        "asr: {provider: inject}\ntts: {provider: mock}\ndev: {allow_text_injection: true}\nobservability: {event_log: false}",
    ).unwrap();
    let runtime = voice_live::VoiceRuntime::build_with_policies(config, None, policies)
        .await
        .unwrap();
    let (tx1, _rx1) = tokio::sync::mpsc::channel(256);
    let (tx2, _rx2) = tokio::sync::mpsc::channel(256);
    let first = runtime.create_session(tx1).await.unwrap();
    let second = runtime.create_session(tx2).await.unwrap();
    assert!(runtime.inject_user_text(first.session_id, "first client request"));
    assert!(runtime.inject_user_text(second.session_id, "second client request"));
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    assert_eq!(factories.load(Ordering::SeqCst), 2);
    let calls = mock.call_history();
    assert!(
        calls
            .iter()
            .any(|call| call.messages[1].content.contains("first client request"))
    );
    assert!(
        calls
            .iter()
            .any(|call| call.messages[1].content.contains("second client request"))
    );
    for call in calls {
        let input = &call.messages[1].content;
        assert!(
            !(input.contains("first client request") && input.contains("second client request"))
        );
    }
    runtime.close_session(first.session_id).await;
    runtime.close_session(second.session_id).await;
}

struct RecordingTts(Arc<parking_lot::Mutex<Vec<String>>>);
#[async_trait]
impl voice_live::tts::StreamingTts for RecordingTts {
    async fn synthesize(
        &self,
        request: voice_live::tts::TtsRequest,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<Box<dyn voice_live::tts::TtsStream>> {
        self.0.lock().push(request.text.clone());
        voice_live::tts::fake::FakeTts::new(24_000, 20, 1)
            .synthesize(request, cancellation)
            .await
    }
}

/// One committed user turn through a real framework agent with a scripted model and the offline `create` tool; returns what reached synthesis.
async fn spoken_through_framework(model_replies: Vec<String>) -> Vec<String> {
    let config = voice_live::VoiceRuntimeConfig::from_yaml_str(
        r#"
observability: {event_log: false}
tools: {effects: {create: transactional_write}}
speech:
  failure_text: "Please say that again."
  outcomes:
    committed:
      operation: create
      class: transactional
      receipt_fields: {status: committed}
      bindings: [target]
"#,
    )
    .unwrap();
    let mut mock = MockLLMProvider::new("offline");
    mock.set_responses(model_replies, false);
    let hooks = VoiceAgentHooks::new();
    let memory: Arc<dyn Memory> = Arc::new(InMemoryStore::new(40));
    let runtime = AgentBuilder::from_yaml(
        "name: OfflineOutcome\nsystem_prompt: Offline outcome test\ntools: [create]\nstreaming: {enabled: true, include_tool_events: false}",
    )
    .unwrap()
    .llm(Arc::new(mock))
    .memory(memory.clone())
    .hooks(hooks.clone() as Arc<dyn AgentHooks>)
    .tool(hooks.bind_tool(Arc::new(Create)))
    .build()
    .unwrap();
    let agent = AiAgentsCognitiveAgent::with_turn_memory(Arc::new(runtime), hooks, memory);
    let texts = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let shutdown = tokio_util::sync::CancellationToken::new();
    let (session, handle) = voice_live::VoiceSessionBuilder::new(
        Arc::new(config),
        Arc::new(voice_live::playback::UnacknowledgedPlaybackSink),
        Arc::new(RecordingTts(texts.clone())),
        Arc::new(agent),
    )
    .with_shutdown(shutdown.clone())
    .build();
    let meta = session.meta_factory.new_meta();
    let worker = tokio::spawn(session.run());
    handle
        .emit(voice_live::VoiceEvent::UserTurnCommitted {
            meta,
            turn_id: voice_live::TurnId::new(),
            transcript_revision: voice_live::Revision(1),
            transcript: "request".into(),
        })
        .await;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    shutdown.cancel();
    worker.await.unwrap().unwrap();
    texts.lock().clone()
}

#[tokio::test(start_paused = true)]
async fn framework_outcome_reply_is_checked_against_executed_tool_without_call_ids() {
    // The model runs the tool, then writes its own reply naming the outcome.
    // It never sees or repeats a call id; the runtime binds the executed record.
    let spoken = spoken_through_framework(vec![
        r#"{"tool":"create","arguments":{"target":"A"}}"#.into(),
        r#"{"text":"Created A.","outcome":"committed"}"#.into(),
    ])
    .await;
    assert_eq!(spoken, ["Created A."]);

    // The same reply without the tool having run is not spoken.
    let spoken = spoken_through_framework(vec![
        r#"{"text":"Created A.","outcome":"committed"}"#.into(),
    ])
    .await;
    assert_eq!(spoken, ["Please say that again."]);
}

/// The built-in framework path takes its tools from the host, so a spec that names a host tool builds with that tool and without any demo backend.
#[tokio::test]
async fn framework_task_agent_uses_the_tools_the_host_supplies() {
    // Building an agent reads the key from the environment but never calls the model.
    unsafe { std::env::set_var("VOICE_LIVE_TEST_TOOL_KEY", "offline") };
    let dir = std::env::temp_dir().join(format!("voice-live-host-tools-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let spec = dir.join("task.yaml");
    std::fs::write(
        &spec,
        "name: HostToolAgent
system_prompt: |
  Use the create tool.
  {{ context.voice.brief }}
tools: [create]
context:
  voice:
    type: runtime
    required: true
llms:
  default:
    provider: openai
    model: gpt-5.4-mini
    api_key_env: VOICE_LIVE_TEST_TOOL_KEY
llm:
  default: default
",
    )
    .unwrap();
    let config = || {
        let mut config = voice_live::VoiceRuntimeConfig::from_yaml_str(
            "asr: {provider: inject}
tts: {provider: mock}
dev: {allow_text_injection: true}
observability: {event_log: false}",
        )
        .unwrap();
        config.agents.task.spec = Some(spec.clone());
        config
    };

    let with_tools = voice_live::VoiceRuntime::build_with_policies(
        config(),
        None,
        voice_live::RuntimePolicies::default().with_task_tools(vec![Arc::new(Create)]),
    )
    .await
    .unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel(16);
    let session = with_tools.create_session(tx).await;
    assert!(session.is_ok(), "{:?}", session.err());

    let without_tools =
        voice_live::VoiceRuntime::build_with_policies(config(), None, Default::default())
            .await
            .unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel(16);
    assert!(
        without_tools.create_session(tx).await.is_err(),
        "no demo tools may stand in for the missing host tool"
    );
}
