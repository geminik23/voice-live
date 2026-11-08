use super::*;
use ai_agents::tools::{ToolExecutionRecord, ToolOperationKind, ToolSafetyMetadata};
use std::sync::atomic::{AtomicUsize, Ordering};

struct OutcomeTool {
    effects: Arc<AtomicUsize>,
    park: bool,
}
#[async_trait]
impl Tool for OutcomeTool {
    fn id(&self) -> &str {
        "create"
    }
    fn name(&self) -> &str {
        "Create"
    }
    fn description(&self) -> &str {
        "Offline test"
    }
    fn input_schema(&self) -> Value {
        serde_json::json!({"type":"object"})
    }
    fn safety_metadata(&self) -> ToolSafetyMetadata {
        ToolSafetyMetadata::read_only(ToolOperationKind::Read)
    }
    async fn execute(&self, args: Value, _: ToolExecutionContext) -> ToolResult {
        self.effects.fetch_add(1, Ordering::SeqCst);
        if self.park {
            std::future::pending::<()>().await;
        }
        ToolResult::ok(args.to_string())
    }
}
fn record(ctx: ToolExecutionContext, result: ToolResult) -> ToolExecutionRecord {
    ToolExecutionRecord {
        call_id: ctx.call_id,
        requested_name: "create".into(),
        canonical_id: "create".into(),
        source: ctx.source,
        arguments: serde_json::json!({"target":"before approval"}),
        executed_arguments: serde_json::json!({"target":"approved"}),
        policy_version: 0,
        registry_version: 0,
        runtime_config_version: 0,
        executed: true,
        success: result.success,
        output: result.output,
        metadata: result.metadata.unwrap_or_default(),
        policy: ctx.permission,
        approval: None,
        started_at: ctx.started_at,
        duration_ms: 1,
        timed_out: false,
        cancelled: false,
        cancellation_reason: None,
        output_truncated: false,
    }
}

#[tokio::test]
async fn logical_origin_survives_amendment_retries_and_is_never_rebound() {
    let hooks = VoiceAgentHooks::new();
    let task = TaskId::new();
    let (tx, mut rx) = mpsc::channel(16);
    let token = hooks.admit(task, 7, tx).unwrap();
    let tool = hooks.bind_tool(Arc::new(OutcomeTool {
        effects: Arc::new(AtomicUsize::new(0)),
        park: false,
    }));
    let mut ctx = ToolExecutionContext::test("create");
    ctx.actor.sender_agent_id = Some(token);
    for _ in 0..2 {
        let result = tool
            .execute(serde_json::json!({"target":"approved"}), ctx.clone())
            .await;
        assert!(result.success);
        assert_eq!(hooks.calls.lock().len(), 1);
    }
    let result = tool
        .execute(serde_json::json!({"target":"approved"}), ctx.clone())
        .await;
    let rec = record(ctx, result);
    hooks.on_tool_execution_record(&rec).await;
    let mut saw_proof = false;
    while let Ok(event) = rx.try_recv() {
        if let CognitiveEvent::InvocationProof {
            task_id,
            thought_epoch,
            proof,
        } = event
        {
            assert_eq!(task_id, task);
            assert_eq!(thought_epoch, 7);
            assert_eq!(proof.arguments, serde_json::json!({"target":"approved"}));
            saw_proof = true;
        }
    }
    assert!(saw_proof);
    hooks.release(task);
    let successor = TaskId::new();
    let (tx2, mut rx2) = mpsc::channel(8);
    hooks.admit(successor, 8, tx2).unwrap();
    hooks.on_tool_execution_record(&rec).await;
    assert!(rx2.try_recv().is_err());
    assert!(rx.try_recv().is_err());
    hooks.release(successor);
    assert!(hooks.origins.lock().is_empty());
}

#[tokio::test]
async fn oversized_identity_emits_bounded_invalid_observation_before_storage() {
    let hooks = VoiceAgentHooks::new();
    let task = TaskId::new();
    let (tx, mut rx) = mpsc::channel(8);
    let token = hooks.admit(task, 1, tx).unwrap();
    let tool = hooks.bind_tool(Arc::new(OutcomeTool {
        effects: Arc::new(AtomicUsize::new(0)),
        park: false,
    }));
    let mut ctx = ToolExecutionContext::test("create");
    ctx.actor.sender_agent_id = Some(token);
    ctx.call_id = "x".repeat(257);
    let result = tool.execute(Value::Null, ctx.clone()).await;
    hooks.on_tool_execution_record(&record(ctx, result)).await;
    let event = rx.try_recv().unwrap();
    assert!(
        matches!(event, CognitiveEvent::InvocationProof { proof, .. } if !proof.bounded() && proof.call_id.is_empty() && proof.output.is_empty())
    );
    assert!(hooks.calls.lock().is_empty());
    hooks.release(task);
}

#[tokio::test]
async fn effect_before_receipt_cancellation_leaves_effect_but_no_completion_proof() {
    let hooks = VoiceAgentHooks::new();
    let task = TaskId::new();
    let (tx, mut rx) = mpsc::channel(8);
    let token = hooks.admit(task, 1, tx).unwrap();
    let effects = Arc::new(AtomicUsize::new(0));
    let tool = hooks.bind_tool(Arc::new(OutcomeTool {
        effects: effects.clone(),
        park: true,
    }));
    let mut ctx = ToolExecutionContext::test("create");
    ctx.actor.sender_agent_id = Some(token);
    let work = tokio::spawn(async move { tool.execute(Value::Null, ctx).await });
    tokio::task::yield_now().await;
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    work.abort();
    let _ = work.await;
    hooks.release(task);
    assert!(hooks.origins.lock().is_empty());
    while let Ok(event) = rx.try_recv() {
        assert!(!matches!(event, CognitiveEvent::InvocationProof { .. }));
    }
    assert_eq!(effects.load(Ordering::SeqCst), 1);
}
