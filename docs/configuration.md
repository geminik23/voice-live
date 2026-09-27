# Configuration reference (voice-runtime.yaml)

All fields are optional and use defaults when omitted. Secrets are not stored in
this file; `api_key_env` looks them up only from environment variables.

```yaml
version: 1

session:
  control_tick_ms: 40        # deterministic tick interval

audio:
  input_sample_rate_hz: 16000   # sample rate sent to ASR
  output_sample_rate_hz: 24000  # TTS playback sample rate
  frame_ms: 20

asr:
  provider: inject          # together | inject | mock
  model: nvidia/nemotron-3.5-asr-streaming-0.6b
  endpoint: wss://api.together.xyz/realtime/v1/audio/transcriptions
  api_key_env: TOGETHER_API_KEY
  chunk_ms: 160
  manual_commit: false     # when true, server VAD controls endpointing and
                           # sends commit_audio() on each LocalSpeechEnded
  locale: ko-KR
  partials:
    enabled: true
    stable_after_ms: 320   # only prefixes unchanged for this long are stable
    stable_revisions: 2    # must match for N consecutive revisions to be stable

tts:
  provider: mock           # qwen | mock
  model: qwen3-tts-flash
  endpoint: wss://dashscope.aliyuncs.com/api/v1/inference/qwen3-tts/realtime
  api_key_env: DASHSCOPE_API_KEY
  voice: ""                # if empty, read from voice_env
  voice_env: QWEN_VOICE_ID # voice IDs are account-specific; do not commit one to config
  sample_rate_hz: 24000
  chunk_ms: 60
  premade:
    enabled: true
    phrases: ["네", "네, 확인했어요."]   # pre-synthesize at startup for immediate playback

turn_control:
  soft_silence_ms: 500      # start evaluating endpointing after this silence
  hard_silence_ms: 1100     # safety-net commit
  require_semantic_completion: true
  barge_in:
    duck_immediately: true
    duck_gain: 0.15
    duck_fade_ms: 60
    confirm_ms: 240         # wait after ducking before classification
  backchannel:
    enabled: true
    minimum_interval_ms: 2500
    maximum_per_user_turn: 2
  hard_stop_phrases: [잠깐, 그만, 멈춰, 취소, ...]
    # validate() rejects the config if it includes "아니"
  incomplete_endings: [는데, 고, 면, 다가, 그게, ...]
  complete_endings: [해 주세요, 할게요, 인가요, 됩니다, 해줘, ...]
  backchannel_words: [네, 응, 어, 아, 음, 그렇군요, ...]
  answer_min_silence_ms: 320   # silence threshold for committing a short answer after a question

agents:
  interaction:
    enabled: false          # when true, use an LLM only for uncertain cases
    spec: ../agents/voice-interaction.yaml
    minimum_interval_ms: 250
    timeout_ms: 900
  task:
    spec: ../agents/reservation.yaml
    timeout_ms: 30000
  deep:
    enabled: false
    spec: ../agents/deep-worker.yaml
    timeout_ms: 60000
  speculative:
    enabled: false          # when true, run a read-only speculative task in a separate runtime instance
    spec: ../agents/reservation-readonly.yaml
    allowed_tools: [search_availability]
    minimum_intent_confidence: 0.85
    maximum_parallel_tasks: 1
    timeout_ms: 5000
    allow_pure_read: true

speech:
  hard_stop_ack_text: "네, 멈췄어요."
  playback_ack_timeout_ms: 15000  # release the speech queue if no playback ACK arrives; 0 disables this
  audible_history_max_clauses: 64 # recent clauses kept by the audible ledger for echo detection and summaries
  progress_templates:
    search_availability:         # tool name is the key
      text: "조건에 맞는 곳을 확인하고 있어요."
      minimum_expected_latency_ms: 1200   # 0 omits the progress message
      expire_after_ms: 4000

tools:
  effects:
    search_availability: pure_read
    reserve_restaurant: transactional_write
    # IdempotentWrite is also supported; only TransactionalWrite creates an Action Commit

vad:
  onset_frames: 3           # consecutive frames (about 60 ms at 20 ms per frame)
  offset_frames: 12         # about 240 ms
  onset_ratio: 4.0          # multiplier of noise_floor
  offset_ratio: 2.0

echo:
  enabled: true
  similarity_threshold: 0.82
  min_chars: 4

observability:
  event_log: true
  event_log_dir: target/voice-traces
  include_sensitive_payloads: false  # default: redact transcript/audio text

dev:
  allow_text_injection: true   # allow the inject_user_text client message
```

## Why the default configuration runs immediately

The sample configuration runs without API keys.

- `asr.provider: inject` — a silent recognizer plus a text-injection channel
- `dev.allow_text_injection: true` — the browser text box acts as the user's utterance
- `tts.provider: mock` — deterministic silent PCM

When `allow_text_injection` is enabled, the injection channel is attached to
**any ASR provider** (`InjectableAsr`). You can therefore use `provider: together`
for live recognition while also reproducing scenarios with injected text. The
injection queues are per-session, so multiple clients do not interfere with
one another.

## Choosing a provider

| Provider | Purpose | Requirements |
|---|---|---|
| `mock` (ASR/TTS) | Offline demo and pipeline checks | None |
| `inject` (ASR) | Development text injection: the browser text box acts as the user's utterance | `dev.allow_text_injection` |
| `together` (ASR) | Live Korean streaming recognition | `TOGETHER_API_KEY` |
| `qwen` (TTS) | Live speech synthesis | `DASHSCOPE_API_KEY`, `QWEN_VOICE_ID` (or `tts.voice`) |

Unknown provider names cause an error at load time; they do not silently fall
back to `mock`.

Verify the wire-level fields of the `together`/`qwen` adapters with the paid
contract tests before use (`cargo test --test provider_contracts -- --ignored`).
The Together test requires a consented 16 kHz mono PCM WAV supplied through
`VOICE_KOREAN_FIXTURE`.

## Validation rules

- `soft_silence_ms` must be less than `hard_silence_ms`.
- Including "아니" in `hard_stop_phrases` fails at load time (to prevent false aborts).
- Enabling `agents.speculative.enabled` requires a dedicated `spec`, a nonempty
  `allowed_tools` list, and `allow_pure_read: true`; every allowlisted tool must
  be `pure_read` in `tools.effects`.

## Audio sample rates

`audio.input_sample_rate_hz` is the rate **sent to the recognizer**. The client
reports its measured rate in `session_start`, and the server resamples between
them.

- Integer ratio (48000 → 16000): anti-alias decimation
- Non-integer ratio (44100 → 16000): low-pass filtering followed by linear interpolation
- Unsupported conversion: raise `ProviderError` and discard the audio (do not
  send it to the provider at the wrong rate)

## Framework integration

When `agents.task.spec` is present, the runtime builds a `RuntimeAgent` through
the `ai-agents` `AgentBuilder` chain and wraps it in `AiAgentsCognitiveAgent`.
It attempts the build once at startup. If that fails with development providers
(`mock`/`inject`), it falls back to `MockReservationAgent`; with live providers,
it fails immediately.

A new brain is created **for each session**. `RuntimeAgent` holds conversation
memory, so sharing one would mix transcripts between clients. Only the tool
backend (`ReservationStore`) is shared.
Interaction, Speculative, and Deep each create a **separate RuntimeAgent
instance** because root turns on one runtime are serialized. Enabling
Speculative requires a dedicated `spec`, a nonempty `allowed_tools` list, and
`allow_pure_read: true`; every allowlisted tool must be declared `pure_read` in
`tools.effects`.

## Task agent YAML contract

voice-live requires two things from the task agent.

**1. Declare the `context.voice` key and render it in the system prompt.**

```yaml
context:
  voice:
    type: runtime
    required: true      # ai-agents checks this at the start of every turn
    # Do not set default: it would always pass validation and could override the value set by voice-live

system_prompt: |
  ...
  Runtime state for this turn:
  {{ context.voice.brief }}
```

`brief` contains `transcript_revision`, `idempotency_key`, `semantic_frame`,
and any speculative/deep results. You can also use
`{{ context.voice.idempotency_key }}` separately.

Who guarantees what:

| Guarantee | Owner |
|---|---|
| Was the key ever set? | ai-agents — `required: true` checks at the start of every turn and fails the turn **before calling the LLM** if the key is absent (the user message is not recorded either). |
| Is the value **fresh** on each turn? | voice-live — the check only tests presence, so an old value from a previous turn would pass. The adapter supplies a new value before each root turn. |
| Does the prompt actually reference it, and is there no `default`? | Demo test `task_agent_declares_and_renders_the_voice_context` — without a reference the value does not appear in the prompt, and a `default` always passes the check. |

**A spec requiring `context.voice` is only for the task agent.** Using the same
spec for an agent that calls `chat()` without voice context (such as the deep
worker) fails every turn with `Required context 'voice' not provided`. The deep
worker therefore uses a dedicated spec (`agents/deep-worker.yaml`).

**2. voice-live owns the `memory:` block.**

Injecting host memory via `AgentBuilder::memory` causes the YAML `memory:` block
to be ignored entirely. voice-live reads only its `max_messages` to create an
`InMemoryStore`; if `type` is not `in-memory`, it warns and substitutes that
store (compacting is not supported).

The speculative agent YAML is exempt from this contract: speculative replies
are not heard by the user, so the framework manages its memory.
