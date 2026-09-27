//! Runs the demo's agent specs through the real ai-agents runtime with a mock
//! LLM, to check each spec fits how voice-live calls it.
//!
//! ai-agents validates `required: true` context keys at the start of every
//! turn. The task agent spec requires `context.voice`, which only the task
//! agent's adapter sets; any other agent calling a spec that requires it fails
//! before reaching the model.

use std::path::PathBuf;
use std::sync::Arc;

use ai_agents::agent::{Agent, AgentBuilder, RuntimeAgent};
use ai_agents_llm::mock::MockLLMProvider;
use voice_live::agent::framework::RuntimeDeepWorker;
use voice_live::{DeepWorkRequest, DeepWorker, TaskId, VoiceRuntimeConfig};

fn demo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn sample_config() -> VoiceRuntimeConfig {
    VoiceRuntimeConfig::from_file(demo_root().join("configs/voice-runtime.yaml"))
        .expect("sample config parses")
}

fn runtime_with_mock(spec: &std::path::Path, reply: &str) -> (RuntimeAgent, MockLLMProvider) {
    let mut mock = MockLLMProvider::new("mock");
    mock.set_responses(vec![reply.to_string()], true);
    let runtime = AgentBuilder::from_yaml_file(spec)
        .expect("spec parses")
        .llm(Arc::new(mock.clone()))
        .build()
        .expect("spec builds with a mock LLM");
    (runtime, mock)
}

/// The deep worker calls `chat()` with no voice context. The spec the sample
/// config gives it must therefore not require one — pointing it back at the
/// task agent's spec fails this test.
#[tokio::test]
async fn deep_worker_spec_runs_without_voice_context() {
    let spec = sample_config().agents.deep.spec.expect("deep worker spec");
    let (runtime, mock) = runtime_with_mock(&spec, "강남 주차 가능 매장 2곳 확인");

    let worker = RuntimeDeepWorker::new(Arc::new(runtime));
    let result = worker
        .run(DeepWorkRequest {
            task_id: TaskId::new(),
            thought_epoch: 1,
            objective: "주차 가능한 매장을 조사".into(),
            committed_context: serde_json::json!("강남에서 4명 예약"),
            dependencies: vec!["area".into()],
        })
        .await
        .unwrap_or_else(|error| panic!("deep worker spec {} failed: {error}", spec.display()));

    assert_eq!(result.summary, "강남 주차 가능 매장 2곳 확인");
    assert_eq!(mock.call_count(), 1);
}

/// Pins the framework behaviour the split relies on: the task agent spec
/// rejects a turn whose voice context was never set, before any model call.
#[tokio::test]
async fn task_agent_spec_rejects_a_turn_without_voice_context() {
    let spec = sample_config().agents.task.spec.expect("task agent spec");
    let (runtime, mock) = runtime_with_mock(&spec, "unused");

    let error = runtime
        .chat("토요일 저녁 7시로 예약해줘")
        .await
        .expect_err("a required context key that was never set must fail the turn");

    assert!(
        error.to_string().contains("Required context 'voice'"),
        "unexpected error: {error}"
    );
    assert_eq!(mock.call_count(), 0, "the model must not be called");
}
