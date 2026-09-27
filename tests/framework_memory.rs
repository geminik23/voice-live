//! Turn memory against the real ai-agents runtime, with a mock LLM.
//!
//! Scenario tests observe what the supervisor *reports*; these observe what
//! the framework actually stores and what the model actually receives. The
//! mock provider's call history shows the exact message list of every LLM
//! call, which is the ground truth for "what the model believes happened".

#![cfg(feature = "framework")]

use std::sync::Arc;
use std::time::Duration;

use ai_agents::agent::AgentBuilder;
use ai_agents::hooks::AgentHooks;
use ai_agents::llm::{ChatMessage, Role};
use ai_agents::memory::{InMemoryStore, Memory};
use ai_agents_llm::mock::MockLLMProvider;

use voice_live::TaskId;
use voice_live::agent::framework::{
    AiAgentsCognitiveAgent, ResolutionOutcome, VoiceAgentHooks, apply_resolution,
};
use voice_live::agent::{
    AgentTurnRequest, CognitiveAgent, CognitiveEvent, TurnContext, TurnResolution,
};

const FIRST_REPLY: &str = "확인했습니다. 토요일 저녁 7시에 두 곳이 가능합니다.";
const SECOND_REPLY: &str = "창가 자리로 알아볼게요.";

fn build_agent(mock: &MockLLMProvider) -> (Arc<dyn CognitiveAgent>, Arc<dyn Memory>) {
    let memory: Arc<dyn Memory> = Arc::new(InMemoryStore::new(40));
    let hooks = VoiceAgentHooks::new();

    let runtime = AgentBuilder::new()
        .system_prompt("You are a test agent.\nRuntime state:\n{{ context.voice.brief }}")
        .llm(Arc::new(mock.clone()))
        .memory(Arc::clone(&memory))
        .hooks(Arc::clone(&hooks) as Arc<dyn AgentHooks>)
        .build()
        .expect("agent builds with a mock LLM");

    let agent =
        AiAgentsCognitiveAgent::with_turn_memory(Arc::new(runtime), hooks, Arc::clone(&memory));

    (Arc::new(agent), memory)
}

fn mock_with(replies: &[&str]) -> MockLLMProvider {
    let mut mock = MockLLMProvider::new("mock");
    mock.set_responses(replies.iter().map(|r| r.to_string()).collect(), false);
    mock
}

fn request(task_id: TaskId, input: &str, brief: &str) -> AgentTurnRequest {
    AgentTurnRequest {
        task_id,
        thought_epoch: 1,
        input: input.to_string(),
        context: TurnContext {
            idempotency_key: format!("voice-turn-{task_id}"),
            brief: brief.to_string(),
        },
        speculative: false,
        dependencies: Vec::new(),
    }
}

async fn run_turn(agent: &Arc<dyn CognitiveAgent>, request: AgentTurnRequest) -> String {
    let mut stream = agent.start_turn(request).await.expect("turn starts");
    loop {
        match stream.next_event().await.expect("stream event") {
            Some(CognitiveEvent::Final { text }) => return text,
            Some(CognitiveEvent::Failed { message }) => panic!("turn failed: {message}"),
            Some(_) => continue,
            None => panic!("stream ended without a final"),
        }
    }
}

fn contents(messages: &[ChatMessage], role: Role) -> Vec<String> {
    messages
        .iter()
        .filter(|m| std::mem::discriminant(&m.role) == std::mem::discriminant(&role))
        .map(|m| m.content.clone())
        .collect()
}

/// H1: the user message is the transcript alone, and the turn environment
/// reaches the model through the system prompt instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn user_message_is_the_transcript_and_context_reaches_the_system_prompt() {
    let mock = mock_with(&[FIRST_REPLY]);
    let (agent, memory) = build_agent(&mock);

    let transcript = "토요일 저녁 7시로 예약해줘";
    let brief = "idempotency_key: voice-turn-abc\nsemantic_frame: {}";
    run_turn(&agent, request(TaskId::new(), transcript, brief)).await;

    let call = mock.last_call().expect("the model was called");
    let system = contents(&call.messages, Role::System).join("\n");
    assert!(
        system.contains("idempotency_key: voice-turn-abc"),
        "turn context must be rendered into the system prompt: {system:?}"
    );

    let users = contents(&call.messages, Role::User);
    assert_eq!(
        users,
        vec![transcript.to_string()],
        "user message must be the transcript only"
    );

    let stored = memory.get_messages(None).await.unwrap();
    assert!(
        stored
            .iter()
            .all(|m| !m.content.contains("idempotency_key")),
        "turn context must never be stored in conversation memory"
    );
}

/// H2: an interrupted reply is rewritten to what was heard, and the next turn's
/// model call sees the heard text, not the generated one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audible_resolution_is_what_the_next_turn_sees() {
    let mock = mock_with(&[FIRST_REPLY, SECOND_REPLY]);
    let (agent, _memory) = build_agent(&mock);

    let first = TaskId::new();
    let final_text = run_turn(&agent, request(first, "토요일 7시 예약해줘", "b")).await;
    assert_eq!(final_text, FIRST_REPLY);

    let heard = "확인했습니다. 토요일 저녁… [중단됨]";
    agent.resolve_turn(
        first,
        TurnResolution::Audible {
            final_text: final_text.clone(),
            heard: heard.to_string(),
        },
    );

    run_turn(&agent, request(TaskId::new(), "창가 자리 있어?", "b")).await;

    let call = mock.last_call().expect("second call");
    let assistants = contents(&call.messages, Role::Assistant);
    assert!(
        assistants.iter().any(|a| a == heard),
        "the heard text must be in the model's history: {assistants:?}"
    );
    assert!(
        assistants.iter().all(|a| a != FIRST_REPLY),
        "the unheard generated reply must be gone: {assistants:?}"
    );
}

/// H2: a discarded turn leaves nothing behind — neither its user message nor
/// its reply.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discard_removes_the_whole_turn() {
    let mock = mock_with(&[FIRST_REPLY, SECOND_REPLY]);
    let (agent, _memory) = build_agent(&mock);

    let first = TaskId::new();
    run_turn(&agent, request(first, "금요일 7시로", "b")).await;
    agent.resolve_turn(first, TurnResolution::Discard);

    run_turn(&agent, request(TaskId::new(), "토요일 7시로", "b")).await;

    let call = mock.last_call().expect("second call");
    assert_eq!(
        contents(&call.messages, Role::User),
        vec!["토요일 7시로".to_string()],
        "the discarded turn's user message must not reach the model"
    );
    assert!(
        contents(&call.messages, Role::Assistant).is_empty(),
        "the discarded turn's reply must not reach the model"
    );
}

/// Rule 1: a new turn is not admitted until the previous one is resolved, so
/// its savepoint can never be taken while the previous turn's memory is still
/// being edited.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn next_turn_waits_for_the_previous_resolution() {
    let mock = mock_with(&[FIRST_REPLY, SECOND_REPLY]);
    let (agent, _memory) = build_agent(&mock);

    let first = TaskId::new();
    run_turn(&agent, request(first, "첫 번째", "b")).await;
    assert_eq!(mock.call_count(), 1);

    let second_agent = Arc::clone(&agent);
    let second = tokio::spawn(async move {
        run_turn(&second_agent, request(TaskId::new(), "두 번째", "b")).await
    });

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        mock.call_count(),
        1,
        "the second turn must not reach the model before the first is resolved"
    );

    agent.resolve_turn(
        first,
        TurnResolution::Audible {
            final_text: FIRST_REPLY.to_string(),
            heard: FIRST_REPLY.to_string(),
        },
    );

    let reply = tokio::time::timeout(Duration::from_secs(5), second)
        .await
        .expect("second turn proceeds once the first is resolved")
        .unwrap();
    assert_eq!(reply, SECOND_REPLY);
    assert_eq!(mock.call_count(), 2);
}

// --- apply_resolution against a bare memory store -------------------------

async fn store_with(messages: &[ChatMessage]) -> Arc<dyn Memory> {
    let memory: Arc<dyn Memory> = Arc::new(InMemoryStore::new(40));
    for message in messages {
        memory.add_message(message.clone()).await.unwrap();
    }
    memory
}

#[tokio::test]
async fn rewrite_targets_this_turns_reply_and_stops_at_its_user_message() {
    let memory = store_with(&[
        ChatMessage::user("a"),
        ChatMessage::assistant("같은 문장"),
        ChatMessage::user("b"),
        ChatMessage::assistant("다른 문장"),
    ])
    .await;
    let savepoint = memory.snapshot().await.unwrap();

    // An identical reply from an *earlier* turn must not be touched.
    let outcome = apply_resolution(
        &*memory,
        savepoint.clone(),
        &TurnResolution::Audible {
            final_text: "같은 문장".into(),
            heard: "바뀐 문장".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(outcome, ResolutionOutcome::FinalNotFound);

    let outcome = apply_resolution(
        &*memory,
        savepoint,
        &TurnResolution::Audible {
            final_text: "다른 문장".into(),
            heard: "다른… [중단됨]".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(outcome, ResolutionOutcome::Rewritten);

    let stored = memory.get_messages(None).await.unwrap();
    assert_eq!(stored[1].content, "같은 문장", "earlier turn untouched");
    assert_eq!(stored[3].content, "다른… [중단됨]");
}

#[tokio::test]
async fn fully_heard_reply_is_left_unchanged() {
    let memory = store_with(&[ChatMessage::user("a"), ChatMessage::assistant("답")]).await;
    let savepoint = memory.snapshot().await.unwrap();

    let outcome = apply_resolution(
        &*memory,
        savepoint,
        &TurnResolution::Audible {
            final_text: "답".into(),
            heard: "답".into(),
        },
    )
    .await
    .unwrap();

    assert_eq!(outcome, ResolutionOutcome::Unchanged);
}

/// Known limitation, pinned so a future fix is noticed. When a state
/// transition regenerates the reply, ai-agents (as of 1.0.10) stores the stale
/// pre-transition text as an assistant message first. Without a message-kind
/// marker it cannot be told apart from a tool-call decision, so it is left in
/// place; only the final reply is rewritten.
#[tokio::test]
async fn stale_pre_transition_text_is_left_in_place() {
    let memory = store_with(&[
        ChatMessage::user("예약해줘"),
        ChatMessage::assistant("이전 상태에서 만든 답"),
        ChatMessage::assistant("최종 답"),
    ])
    .await;
    let savepoint = memory.snapshot().await.unwrap();

    apply_resolution(
        &*memory,
        savepoint,
        &TurnResolution::Audible {
            final_text: "최종 답".into(),
            heard: "최종… [중단됨]".into(),
        },
    )
    .await
    .unwrap();

    let stored = memory.get_messages(None).await.unwrap();
    assert_eq!(stored[1].content, "이전 상태에서 만든 답");
    assert_eq!(stored[2].content, "최종… [중단됨]");
}
