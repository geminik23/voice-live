use std::path::PathBuf;

use ai_agents::agent::AgentBuilder;
use voice_live::VoiceRuntimeConfig;

fn demo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn dual_cartesia_sample_wires_both_providers_and_relative_task_spec() {
    let config =
        VoiceRuntimeConfig::from_file(demo_root().join("configs/voice-runtime.cartesia.yaml"))
            .unwrap();
    assert_eq!(config.asr.provider, "cartesia");
    assert_eq!(config.asr.endpoint, "wss://api.cartesia.ai/stt/websocket");
    assert_eq!(config.asr.model, "ink-whisper");
    assert_eq!(config.asr.api_key_env, "CARTESIA_API_KEY");
    assert_eq!(config.asr.locale, "ko-KR");
    assert!(config.asr.manual_commit);
    assert_eq!(config.asr.chunk_ms, 100);
    assert_eq!(config.asr.cartesia.api_version, "2026-08-14");
    assert_eq!(config.asr.cartesia.finalize_timeout_ms, 2000);
    assert_eq!(config.asr.cartesia.finish_timeout_ms, 5000);
    assert_eq!(config.asr.cartesia.max_transcript_bytes, 65536);
    assert_eq!(config.tts.provider, "cartesia");
    assert_eq!(config.tts.api_key_env, "CARTESIA_API_KEY");
    assert_eq!(config.tts.voice_env, "CARTESIA_VOICE_ID");
    assert!(!config.tts.premade.enabled);
    assert!(config.dev.allow_text_injection);
    let spec = config.agents.task.spec.unwrap();
    assert!(spec.exists());
    AgentBuilder::from_yaml_file(spec).unwrap();
    let legacy = VoiceRuntimeConfig::from_yaml_str("version: 1").unwrap();
    assert_eq!(legacy.asr.cartesia.finalize_timeout_ms, 2000);
    assert_eq!(legacy.asr.cartesia.finish_timeout_ms, 5000);
}

#[test]
fn cartesia_tts_sample_is_explicit_and_preserves_keyless_defaults() {
    let config =
        VoiceRuntimeConfig::from_file(demo_root().join("configs/voice-runtime.cartesia-tts.yaml"))
            .unwrap();
    assert_eq!(config.asr.provider, "inject");
    assert!(config.dev.allow_text_injection);
    assert_eq!(config.tts.provider, "cartesia");
    assert_eq!(config.tts.endpoint, "wss://api.cartesia.ai/tts/websocket");
    assert_eq!(config.tts.model, "sonic-3.6");
    assert_eq!(config.tts.api_key_env, "CARTESIA_API_KEY");
    assert_eq!(config.tts.voice_env, "CARTESIA_VOICE_ID");
    assert_eq!(config.tts.language, "ko");
    assert_eq!(config.tts.sample_rate_hz, 24000);
    assert_eq!(config.tts.cartesia.api_version, "2026-08-14");
    assert_eq!(config.tts.cartesia.max_buffer_delay_ms, 0);
    assert!(!config.tts.premade.enabled);
    let spec = config.agents.task.spec.unwrap();
    assert!(spec.exists());
    AgentBuilder::from_yaml_file(spec).unwrap();
    let keyless =
        VoiceRuntimeConfig::from_file(demo_root().join("configs/voice-runtime.yaml")).unwrap();
    assert_eq!(keyless.asr.provider, "inject");
    assert_eq!(keyless.tts.provider, "mock");
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
