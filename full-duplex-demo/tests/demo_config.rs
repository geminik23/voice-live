use std::path::PathBuf;

use ai_agents::agent::AgentBuilder;
use voice_live::VoiceRuntimeConfig;

fn demo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn demo_runtime_and_agent_yaml_files_parse() {
    let root = demo_root();
    let runtime_path = root.join("configs/voice-runtime.yaml");
    let config = VoiceRuntimeConfig::from_file(&runtime_path).expect("runtime config parses");

    let task_spec = config.agents.task.spec.expect("task agent spec");
    let interaction_spec = config
        .agents
        .interaction
        .spec
        .expect("interaction agent spec");
    let speculative_spec = config
        .agents
        .speculative
        .spec
        .expect("speculative agent spec");

    assert!(task_spec.exists(), "{}", task_spec.display());
    assert!(interaction_spec.exists(), "{}", interaction_spec.display());
    assert!(speculative_spec.exists(), "{}", speculative_spec.display());

    AgentBuilder::from_yaml_file(task_spec).expect("task YAML matches ai-agents schema");
    AgentBuilder::from_yaml_file(interaction_spec)
        .expect("interaction YAML matches ai-agents schema");
    AgentBuilder::from_yaml_file(speculative_spec)
        .expect("speculative YAML matches ai-agents schema");
}

/// ai-agents enforces that a required runtime key was set, but not that the
/// system prompt references it or that no `default` masks it. This is the guard that
/// the task agent actually receives the turn environment voice-live sets.
#[test]
fn task_agent_declares_and_renders_the_voice_context() {
    let raw = std::fs::read_to_string(demo_root().join("agents/reservation.yaml")).unwrap();
    let spec: serde_yaml::Value = serde_yaml::from_str(&raw).unwrap();

    let voice = &spec["context"]["voice"];
    assert_eq!(
        voice["type"].as_str(),
        Some("runtime"),
        "context.voice must be runtime-sourced"
    );
    assert!(
        voice.get("default").is_none(),
        "a default could overwrite the value voice-live sets before each turn"
    );

    let prompt = spec["system_prompt"].as_str().expect("system_prompt");
    assert!(
        prompt.contains("{{ context.voice.brief }}"),
        "the system prompt must render the turn context"
    );

    // The old channel put runtime state in the user message and asked the
    // model to trust an injected history. Neither exists any more.
    for stale in [
        "audible_history",
        "committed_user_turn",
        "voice_runtime_context",
    ] {
        assert!(
            !prompt.contains(stale),
            "system prompt still refers to {stale}"
        );
    }
}
