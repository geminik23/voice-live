use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VoiceRuntimeConfig {
    #[serde(default = "default_version")]
    pub version: u32,

    #[serde(default)]
    pub session: SessionConfig,

    #[serde(default)]
    pub audio: AudioConfig,

    #[serde(default)]
    pub asr: AsrConfig,

    #[serde(default)]
    pub tts: TtsConfig,

    #[serde(default)]
    pub turn_control: TurnControlConfig,

    #[serde(default)]
    pub agents: AgentsConfig,

    #[serde(default)]
    pub speech: SpeechConfig,

    #[serde(default)]
    pub tools: ToolsConfig,

    #[serde(default)]
    pub vad: VadConfig,

    #[serde(default)]
    pub echo: EchoConfig,

    #[serde(default)]
    pub observability: ObservabilityConfig,

    #[serde(default)]
    pub dev: DevConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionConfig {
    #[serde(default = "default_control_tick_ms")]
    pub control_tick_ms: u64,
}

impl Default for SessionConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default session config")
    }
}

impl Default for AudioConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default audio config")
    }
}

impl Default for AsrConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default asr config")
    }
}

impl Default for AsrPartialsConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default partials config")
    }
}

impl Default for TtsConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default tts config")
    }
}

impl Default for PremadeConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default premade config")
    }
}

impl Default for InteractionAgentConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default interaction config")
    }
}

impl Default for VoiceRuntimeConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default runtime config")
    }
}

fn default_version() -> u32 {
    1
}

fn default_control_tick_ms() -> u64 {
    40
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioConfig {
    #[serde(default = "default_input_sample_rate")]
    pub input_sample_rate_hz: u32,
    #[serde(default = "default_output_sample_rate")]
    pub output_sample_rate_hz: u32,
    #[serde(default = "default_frame_ms")]
    pub frame_ms: u64,
}

fn default_input_sample_rate() -> u32 {
    16_000
}

fn default_output_sample_rate() -> u32 {
    24_000
}

fn default_frame_ms() -> u64 {
    20
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsrConfig {
    #[serde(default = "default_asr_provider")]
    pub provider: String,
    #[serde(default = "default_asr_model")]
    pub model: String,
    #[serde(default = "default_asr_endpoint")]
    pub endpoint: String,
    #[serde(default = "default_asr_api_key_env")]
    pub api_key_env: String,
    #[serde(default = "default_chunk_ms")]
    pub chunk_ms: u64,
    #[serde(default)]
    pub manual_commit: bool,
    #[serde(default = "default_asr_locale")]
    pub locale: String,
    /// Deadline for one provider connect/auth/ready attempt.
    #[serde(default = "default_asr_open_timeout_ms")]
    pub open_timeout_ms: u64,
    /// Per-write deadline for audio and commit writes to the provider.
    #[serde(default = "default_asr_write_timeout_ms")]
    pub write_timeout_ms: u64,
    /// Normalized audio queued for the provider writer, converted to samples
    /// at the ASR input rate.
    #[serde(default = "default_asr_input_buffer_ms")]
    pub input_buffer_ms: u64,
    #[serde(default)]
    pub partials: AsrPartialsConfig,
}

fn default_asr_provider() -> String {
    "mock".into()
}

fn default_asr_model() -> String {
    "nvidia/nemotron-3.5-asr-streaming-0.6b".into()
}

fn default_asr_endpoint() -> String {
    "wss://api.together.ai/v1/realtime".into()
}

fn default_asr_api_key_env() -> String {
    "TOGETHER_API_KEY".into()
}

fn default_chunk_ms() -> u64 {
    160
}

fn default_asr_locale() -> String {
    "ko-KR".into()
}

fn default_asr_open_timeout_ms() -> u64 {
    10_000
}

fn default_asr_write_timeout_ms() -> u64 {
    2_000
}

fn default_asr_input_buffer_ms() -> u64 {
    1_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsrPartialsConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_stable_after_ms")]
    pub stable_after_ms: u64,
    #[serde(default = "default_stable_revisions")]
    pub stable_revisions: u32,
}

fn default_stable_after_ms() -> u64 {
    320
}

fn default_stable_revisions() -> u32 {
    2
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtsConfig {
    #[serde(default = "default_tts_provider")]
    pub provider: String,
    #[serde(default = "default_tts_model")]
    pub model: String,
    #[serde(default = "default_tts_endpoint")]
    pub endpoint: String,
    #[serde(default = "default_tts_api_key_env")]
    pub api_key_env: String,
    #[serde(default)]
    pub voice: String,
    /// Environment variable consulted when `voice` is empty. Voice ids are
    /// account-scoped, so they belong beside the API key rather than in a
    /// committed config file.
    #[serde(default = "default_tts_voice_env")]
    pub voice_env: String,
    #[serde(default = "default_tts_sample_rate")]
    pub sample_rate_hz: u32,
    #[serde(default = "default_tts_chunk_ms")]
    pub chunk_ms: u64,
    /// Language sent with synthesis and premade warmup requests. The default
    /// keeps the existing Korean-focused behaviour.
    #[serde(default = "default_tts_language")]
    pub language: String,
    /// Deadline for one text-session connect/auth/ready attempt.
    #[serde(default = "default_tts_open_timeout_ms")]
    pub open_timeout_ms: u64,
    /// Deadline for one response after its input is admitted.
    #[serde(default = "default_tts_request_timeout_ms")]
    pub request_timeout_ms: u64,
    /// Maximum accumulated text bytes for one response.
    #[serde(default = "default_tts_max_input_bytes")]
    pub max_input_bytes: usize,
    #[serde(default)]
    pub premade: PremadeConfig,
}

fn default_tts_provider() -> String {
    "mock".into()
}

fn default_tts_model() -> String {
    "qwen3-tts-flash".into()
}

fn default_tts_endpoint() -> String {
    "wss://dashscope.aliyuncs.com/api/v1/inference/qwen3-tts/realtime".into()
}

fn default_tts_api_key_env() -> String {
    "DASHSCOPE_API_KEY".into()
}

fn default_tts_voice_env() -> String {
    "QWEN_VOICE_ID".into()
}

fn default_tts_sample_rate() -> u32 {
    24_000
}

fn default_tts_chunk_ms() -> u64 {
    60
}

fn default_tts_language() -> String {
    "Korean".into()
}

fn default_tts_open_timeout_ms() -> u64 {
    10_000
}

fn default_tts_request_timeout_ms() -> u64 {
    30_000
}

fn default_tts_max_input_bytes() -> usize {
    65_536
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PremadeConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub phrases: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnControlConfig {
    #[serde(default = "default_soft_silence_ms")]
    pub soft_silence_ms: u64,
    #[serde(default = "default_hard_silence_ms")]
    pub hard_silence_ms: u64,
    #[serde(default = "default_true")]
    pub require_semantic_completion: bool,
    #[serde(default)]
    pub barge_in: BargeInConfig,
    #[serde(default)]
    pub backchannel: BackchannelConfig,
    #[serde(default = "default_hard_stop_phrases")]
    pub hard_stop_phrases: Vec<String>,
    #[serde(default = "default_incomplete_endings")]
    pub incomplete_endings: Vec<String>,
    #[serde(default = "default_complete_endings")]
    pub complete_endings: Vec<String>,
    #[serde(default = "default_backchannel_words")]
    pub backchannel_words: Vec<String>,
    #[serde(default = "default_answer_min_silence_ms")]
    pub answer_min_silence_ms: u64,
}

fn default_soft_silence_ms() -> u64 {
    500
}

fn default_hard_silence_ms() -> u64 {
    1_100
}

fn default_answer_min_silence_ms() -> u64 {
    320
}

fn default_hard_stop_phrases() -> Vec<String> {
    vec![
        "잠깐".into(),
        "잠깐만".into(),
        "그만".into(),
        "멈춰".into(),
        "멈추세요".into(),
        "취소".into(),
        "취소해".into(),
        "정정할게".into(),
    ]
}

fn default_incomplete_endings() -> Vec<String> {
    vec![
        "는데".into(),
        "는데요".into(),
        "인데".into(),
        "하고".into(),
        "라서".into(),
        "다가".into(),
        "기는 한데".into(),
        "그게".into(),
        "그러니까".into(),
        "그니까".into(),
        "음...".into(),
        "...".into(),
    ]
}

fn default_complete_endings() -> Vec<String> {
    vec![
        "해 주세요".into(),
        "해주세요".into(),
        "할게요".into(),
        "할게".into(),
        "인가요".into(),
        "맞나요".into(),
        "맞아요".into(),
        "로 해주세요".into(),
        "로 해 주세요".into(),
        "해줘".into(),
        "해 줘".into(),
        "면 됩니다".into(),
        "됩니다".into(),
        "세요".into(),
        "예요".into(),
        "이에요".into(),
        "습니다".into(),
    ]
}

fn default_backchannel_words() -> Vec<String> {
    vec![
        "네".into(),
        "네네".into(),
        "응".into(),
        "어".into(),
        "아".into(),
        "음".into(),
        "그렇군요".into(),
        "그렇군".into(),
        "맞아요".into(),
        "그래".into(),
        "예".into(),
        "알겠습니다".into(),
        "아 그렇군요".into(),
    ]
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BargeInConfig {
    #[serde(default = "default_true")]
    pub duck_immediately: bool,
    #[serde(default = "default_duck_gain")]
    pub duck_gain: f32,
    #[serde(default = "default_duck_fade_ms")]
    pub duck_fade_ms: u32,
    #[serde(default = "default_confirm_ms")]
    pub confirm_ms: u64,
}

impl Default for BargeInConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default config")
    }
}

fn default_duck_gain() -> f32 {
    0.15
}

fn default_duck_fade_ms() -> u32 {
    60
}

fn default_confirm_ms() -> u64 {
    240
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackchannelConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_backchannel_interval_ms")]
    pub minimum_interval_ms: u64,
    #[serde(default = "default_backchannel_max")]
    pub maximum_per_user_turn: u32,
}

fn default_backchannel_interval_ms() -> u64 {
    2_500
}

fn default_backchannel_max() -> u32 {
    2
}

impl Default for BackchannelConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default config")
    }
}

impl Default for TurnControlConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default config")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentsConfig {
    #[serde(default)]
    pub interaction: InteractionAgentConfig,
    #[serde(default)]
    pub task: TaskAgentConfig,
    #[serde(default)]
    pub deep: DeepAgentConfig,
    #[serde(default)]
    pub speculative: SpeculativeConfig,
}

impl Default for AgentsConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default config")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InteractionAgentConfig {
    #[serde(default)]
    pub enabled: bool,
    pub spec: Option<PathBuf>,
    #[serde(default = "default_interaction_interval_ms")]
    pub minimum_interval_ms: u64,
    #[serde(default = "default_interaction_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_interaction_interval_ms() -> u64 {
    250
}

fn default_interaction_timeout_ms() -> u64 {
    900
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAgentConfig {
    pub spec: Option<PathBuf>,
    #[serde(default = "default_task_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_task_timeout_ms() -> u64 {
    30_000
}

impl Default for TaskAgentConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default config")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeepAgentConfig {
    #[serde(default)]
    pub enabled: bool,
    pub spec: Option<PathBuf>,
    #[serde(default = "default_deep_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_deep_timeout_ms() -> u64 {
    60_000
}

impl Default for DeepAgentConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default config")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeculativeConfig {
    #[serde(default)]
    pub enabled: bool,
    pub spec: Option<PathBuf>,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default = "default_speculative_confidence")]
    pub minimum_intent_confidence: f32,
    #[serde(default = "default_speculative_parallel")]
    pub maximum_parallel_tasks: u32,
    #[serde(default = "default_speculative_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub allow_pure_read: bool,
}

fn default_speculative_confidence() -> f32 {
    0.85
}

fn default_speculative_parallel() -> u32 {
    1
}

fn default_speculative_timeout_ms() -> u64 {
    5_000
}

impl Default for SpeculativeConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default config")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeechConfig {
    #[serde(default)]
    pub progress_templates: HashMap<String, ProgressTemplate>,
    #[serde(default = "default_hard_stop_ack")]
    pub hard_stop_ack_text: String,
    /// How long to wait for a playback acknowledgement before releasing the
    /// speech queue. Speech state only advances on a client ACK, so a
    /// disconnected client would otherwise pin the queue forever. `0` disables.
    #[serde(default = "default_playback_ack_timeout_ms")]
    pub playback_ack_timeout_ms: u64,
    /// How long to wait for an interrupted acknowledgement after an Abort
    /// before settling with the already confirmed progress.
    #[serde(default = "default_playback_stop_ack_timeout_ms")]
    pub playback_stop_ack_timeout_ms: u64,
    /// Recent clauses the audible ledger keeps for self-echo detection and
    /// the session summary. Durable history lives in the agent's memory.
    #[serde(default = "default_audible_history_max_clauses")]
    pub audible_history_max_clauses: usize,
}

fn default_hard_stop_ack() -> String {
    "네, 멈췄어요.".into()
}

fn default_playback_ack_timeout_ms() -> u64 {
    15_000
}

fn default_playback_stop_ack_timeout_ms() -> u64 {
    1_000
}

fn default_audible_history_max_clauses() -> usize {
    crate::speech::ledger::DEFAULT_AUDIBLE_HISTORY_MAX_CLAUSES
}

impl Default for SpeechConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default config")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressTemplate {
    pub text: String,
    #[serde(default)]
    pub minimum_expected_latency_ms: u64,
    #[serde(default = "default_expire_after_ms")]
    pub expire_after_ms: u64,
}

fn default_expire_after_ms() -> u64 {
    4_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolsConfig {
    #[serde(default)]
    pub effects: HashMap<String, ToolEffect>,
}

impl Default for ToolsConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default config")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolEffect {
    PureRead,
    IdempotentWrite,
    TransactionalWrite,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VadConfig {
    #[serde(default = "default_onset_frames")]
    pub onset_frames: u32,
    #[serde(default = "default_offset_frames")]
    pub offset_frames: u32,
    #[serde(default = "default_onset_ratio")]
    pub onset_ratio: f32,
    #[serde(default = "default_offset_ratio")]
    pub offset_ratio: f32,
}

fn default_onset_frames() -> u32 {
    3
}

fn default_offset_frames() -> u32 {
    12
}

fn default_onset_ratio() -> f32 {
    4.0
}

fn default_offset_ratio() -> f32 {
    2.0
}

impl Default for VadConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default config")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EchoConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_similarity_threshold")]
    pub similarity_threshold: f32,
    #[serde(default = "default_echo_min_chars")]
    pub min_chars: usize,
}

fn default_similarity_threshold() -> f32 {
    0.82
}

fn default_echo_min_chars() -> usize {
    4
}

impl Default for EchoConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default config")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    #[serde(default = "default_true")]
    pub event_log: bool,
    #[serde(default = "default_event_log_dir")]
    pub event_log_dir: PathBuf,
    #[serde(default)]
    pub include_sensitive_payloads: bool,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default config")
    }
}

fn default_event_log_dir() -> PathBuf {
    PathBuf::from("target/voice-traces")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DevConfig {
    #[serde(default)]
    pub allow_text_injection: bool,
}

impl Default for DevConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("default config")
    }
}

impl TtsConfig {
    /// The voice to synthesize with: the explicit config value, otherwise the
    /// value of `voice_env`, otherwise empty (provider default).
    pub fn resolved_voice(&self) -> String {
        if !self.voice.trim().is_empty() {
            return self.voice.trim().to_string();
        }

        if self.voice_env.trim().is_empty() {
            return String::new();
        }

        std::env::var(self.voice_env.trim()).unwrap_or_default()
    }
}

impl VoiceRuntimeConfig {
    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path)?;
        let mut config: VoiceRuntimeConfig = serde_yaml::from_str(&raw)?;
        if let Some(base_dir) = path.parent() {
            config.resolve_relative_paths(base_dir);
        }
        config.validate()?;
        Ok(config)
    }

    pub fn from_yaml_str(raw: &str) -> anyhow::Result<Self> {
        let config: VoiceRuntimeConfig = serde_yaml::from_str(raw)?;
        config.validate()?;
        Ok(config)
    }

    fn resolve_relative_paths(&mut self, base_dir: &Path) {
        let resolve = |path: &mut Option<PathBuf>| {
            if let Some(spec) = path
                && spec.is_relative()
            {
                *spec = base_dir.join(&*spec);
            }
        };

        resolve(&mut self.agents.interaction.spec);
        resolve(&mut self.agents.task.spec);
        resolve(&mut self.agents.deep.spec);
        resolve(&mut self.agents.speculative.spec);
    }

    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.audio.input_sample_rate_hz > 0 && self.tts.sample_rate_hz > 0,
            "audio sample rates must be positive"
        );
        let samples = (self.audio.input_sample_rate_hz as u64)
            .checked_mul(self.asr.input_buffer_ms)
            .ok_or_else(|| anyhow::anyhow!("asr input sample budget overflow"))?
            / 1_000;
        anyhow::ensure!(
            samples > 0 && samples <= u32::MAX as u64,
            "asr sample budget must fit a positive u32"
        );
        for millis in [
            self.asr.open_timeout_ms,
            self.asr.write_timeout_ms,
            self.tts.open_timeout_ms,
            self.tts.request_timeout_ms,
            self.speech.playback_ack_timeout_ms,
        ] {
            anyhow::ensure!(millis <= u64::MAX / 1_000, "deadline is too large");
        }
        if self.turn_control.soft_silence_ms >= self.turn_control.hard_silence_ms {
            anyhow::bail!("turn_control.soft_silence_ms must be below hard_silence_ms");
        }

        if self
            .turn_control
            .hard_stop_phrases
            .iter()
            .any(|p| p.trim() == "아니")
        {
            anyhow::bail!(
                "turn_control.hard_stop_phrases must not contain '아니'; it is a common \
                 discourse marker and is handled as a correction cue instead"
            );
        }

        // Local runtime safety limits. These apply without the framework
        // feature, and zero never means disabled for a deadline.
        for (key, value) in [
            ("asr.open_timeout_ms", self.asr.open_timeout_ms),
            ("asr.write_timeout_ms", self.asr.write_timeout_ms),
            ("asr.input_buffer_ms", self.asr.input_buffer_ms),
            ("tts.open_timeout_ms", self.tts.open_timeout_ms),
            ("tts.request_timeout_ms", self.tts.request_timeout_ms),
        ] {
            if value == 0 {
                anyhow::bail!("{key} must be positive");
            }
        }

        if self.tts.max_input_bytes == 0 {
            anyhow::bail!("tts.max_input_bytes must be positive");
        }

        // The stop acknowledgement must stay well below the adapter's 10s
        // turn resolution grace, or a stopped speech could outlive it.
        if self.speech.playback_stop_ack_timeout_ms == 0
            || self.speech.playback_stop_ack_timeout_ms > 5_000
        {
            anyhow::bail!("speech.playback_stop_ack_timeout_ms must be between 1 and 5000");
        }

        if self.agents.speculative.enabled {
            if !self.agents.speculative.allow_pure_read {
                anyhow::bail!("speculative execution requires allow_pure_read: true");
            }
            if self.agents.speculative.spec.is_none() {
                anyhow::bail!("speculative execution requires a dedicated agent spec");
            }
            if self.agents.speculative.allowed_tools.is_empty() {
                anyhow::bail!("speculative execution requires a non-empty allowed_tools list");
            }
            for tool in &self.agents.speculative.allowed_tools {
                if self.tools.effects.get(tool) != Some(&ToolEffect::PureRead) {
                    anyhow::bail!(
                        "speculative tool '{tool}' must be declared pure_read in tools.effects"
                    );
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_agent_specs_relative_to_config_file() {
        let root = std::env::temp_dir().join(format!("voice-config-{}", uuid::Uuid::new_v4()));
        let configs = root.join("configs");
        std::fs::create_dir_all(&configs).unwrap();
        let path = configs.join("voice-runtime.yaml");
        std::fs::write(
            &path,
            r#"
version: 1
agents:
  task:
    spec: ../agents/reservation.yaml
"#,
        )
        .unwrap();

        let config = VoiceRuntimeConfig::from_file(&path).unwrap();

        assert_eq!(
            config.agents.task.spec,
            Some(configs.join("../agents/reservation.yaml"))
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_common_discourse_marker_as_hard_stop() {
        let error = VoiceRuntimeConfig::from_yaml_str(
            r#"
version: 1
turn_control:
  hard_stop_phrases: ["아니"]
"#,
        )
        .expect_err("아니 must not be an immediate abort phrase");

        assert!(error.to_string().contains("아니"));
    }

    #[test]
    fn rejects_speculation_without_dedicated_read_only_contract() {
        let error = VoiceRuntimeConfig::from_yaml_str(
            r#"
version: 1
agents:
  speculative:
    enabled: true
    allow_pure_read: true
    spec: speculative.yaml
"#,
        )
        .expect_err("empty tool allowlist must be rejected");

        assert!(error.to_string().contains("allowed_tools"));
    }

    #[test]
    fn rejects_write_tool_in_speculative_allowlist() {
        let error = VoiceRuntimeConfig::from_yaml_str(
            r#"
version: 1
agents:
  speculative:
    enabled: true
    allow_pure_read: true
    spec: speculative.yaml
    allowed_tools: [reserve_restaurant]
tools:
  effects:
    reserve_restaurant: transactional_write
"#,
        )
        .expect_err("write tool must never enter speculation");

        assert!(error.to_string().contains("pure_read"));
    }
}
