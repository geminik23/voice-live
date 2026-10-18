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
    let proposal = r#"{"response":"ask"}"#;
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
    let runtime = AgentBuilder::from_yaml(
        "name: OfflineProof\nsystem_prompt: Offline proof test\ntools: [create]\nstreaming: {enabled: true, include_tool_events: false}",
    )
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
            Some(CognitiveEvent::ToolCompleted { tool, .. }) => {
                assert_eq!(tool, "create");
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
    let policies = voice_live::RuntimePolicies {
        interaction_brain: Some(Arc::new(move || {
            count.fetch_add(1, Ordering::SeqCst);
            Arc::new(voice_live::agent::framework::RuntimeSemanticBrain::new(
                Arc::new(provider.clone()),
                "Interpret the current speech.".into(),
            )) as Arc<dyn InteractionBrain>
        })),
        ..Default::default()
    };
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
