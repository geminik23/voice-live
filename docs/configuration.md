# Configuration reference (voice-runtime.yaml)

All fields are optional and use defaults when omitted. Secrets are not stored in this file; `api_key_env` looks them up only from environment variables on the built-in provider path.

```yaml
version: 1

session:
  control_tick_ms: 40        # deterministic tick interval

audio:
  input_sample_rate_hz: 16000   # sample rate sent to ASR
  output_sample_rate_hz: 24000  # TTS playback sample rate
  frame_ms: 20

asr:
  provider: inject          # together | cartesia | inject | mock
  model: nvidia/nemotron-3.5-asr-streaming-0.6b
  endpoint: wss://api.together.ai/v1/realtime
  api_key_env: TOGETHER_API_KEY
  chunk_ms: 160
  manual_commit: false     # when true, server VAD controls endpointing and
                           # admits a FIFO provider commit after the frame's PCM
  locale: ko-KR
  open_timeout_ms: 10000
  write_timeout_ms: 2000
  input_buffer_ms: 1000
  cartesia:                 # used only by the Cartesia STT factory
    api_version: "2026-08-14"
    finalize_timeout_ms: 2000
    finish_timeout_ms: 5000
    max_transcript_bytes: 65536
  partials:
    enabled: true
    stable_after_ms: 320   # only prefixes unchanged for this long are stable
    stable_revisions: 2    # must match for N consecutive revisions to be stable

tts:
  provider: mock           # qwen | cartesia | mock
  model: qwen3-tts-flash
  endpoint: wss://dashscope.aliyuncs.com/api/v1/inference/qwen3-tts/realtime
  api_key_env: DASHSCOPE_API_KEY
  voice: ""                # if empty, read from voice_env
  voice_env: QWEN_VOICE_ID # voice IDs are account-specific; do not commit one to config
  sample_rate_hz: 24000
  chunk_ms: 60
  language: Korean
  open_timeout_ms: 10000
  request_timeout_ms: 30000
  max_input_bytes: 65536
  cartesia:                 # used only by the Cartesia TTS factory
    api_version: "2026-08-14"
    max_buffer_delay_ms: 0
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
  playback_stop_ack_timeout_ms: 1000 # bounded interrupted ACK wait after Abort
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

When `allow_text_injection` is enabled, the runtime owns a per-session injection queue independently of the recognizer connection. Typed finals therefore keep flowing during ASR open failures and reconnects, without crossing between clients. `InjectableAsr` remains available as a standalone decorator; the runtime no longer ties injection delivery to that decorator's open.

## Choosing a provider

| Provider | Purpose | Requirements |
|---|---|---|
| `mock` (ASR/TTS) | Offline demo and pipeline checks | None |
| `inject` (ASR) | Development text injection: the browser text box acts as the user's utterance | `dev.allow_text_injection` |
| `together` (ASR) | Live streaming recognition | `TOGETHER_API_KEY` |
| `qwen` (TTS) | Live speech synthesis | `DASHSCOPE_API_KEY`, `QWEN_VOICE_ID` (or `tts.voice`) |
| `cartesia` (ASR) | Ink Whisper manual streaming recognition | Explicit Cartesia endpoint/model/env; English or Korean, 16 kHz, `manual_commit: true`, `CARTESIA_API_KEY` |
| `cartesia` (TTS) | Native incremental Sonic synthesis | Explicit Cartesia endpoint/model/env settings; `CARTESIA_API_KEY`, `CARTESIA_VOICE_ID` (or `tts.voice`) |

Unknown provider names fail the built-in runtime build; they do not silently fall back to `mock`. The injected builder uses the host's factories instead of resolving these names.

Validate each real provider with its individual ignored contract only after credential, cost, and external-transmission approval. Do not run all ignored tests as a keyless check. The Together and Cartesia STT contracts require a consented 16 kHz mono PCM WAV supplied through `VOICE_KOREAN_FIXTURE`; the Cartesia TTS contract sends synthetic Korean text and requires an approved voice. See [provider testing](testing.md#cartesia-provider-regressions-and-paid-contracts).

## Cartesia STT

Use `full-duplex-demo/configs/voice-runtime.cartesia.yaml` for Cartesia STT and TTS together. Set `asr.provider: cartesia`, endpoint `wss://api.cartesia.ai/stt/websocket`, model `ink-whisper`, `api_key_env: CARTESIA_API_KEY`, `manual_commit: true`, locale `ko-KR`, and `audio.input_sample_rate_hz: 16000`. Korean `ko`/`ko-KR` is normalized to `ko`; English `en`/`en-US`/`en-GB` is normalized to `en` and sent from the session locale. Other models, locales, rates, or automatic commit are rejected rather than silently substituted. Provider-name changes alone retain Together defaults and are not a valid migration.

| Key | Default | Validation and use |
|---|---:|---|
| `asr.cartesia.api_version` | `2026-08-14` | Currently the only accepted version, sent in `Cartesia-Version` |
| `asr.cartesia.finalize_timeout_ms` | 2000 | Positive bounded duration; each finalize has a write deadline, then an independent absolute acknowledgement deadline from write completion |
| `asr.cartesia.finish_timeout_ms` | 5000 | Positive bounded duration from the first successful finish admission, including queued PCM, pending flushes, close write, and done |
| `asr.cartesia.max_transcript_bytes` | 65536 | Positive UTF-8 byte limit for confirmed prefix plus mutable tail and each delivered transcript |

The factory validates its own settings and credentials; unused Cartesia settings do not reject a host-injected provider. Empty nested settings use serde defaults. Endpoints must omit userinfo and query parameters; the adapter owns the model/encoding/sample-rate/language query. WSS is required outside loopback. Authentication and successful WebSocket upgrade establish send-readiness; no application-level ready event is invented.

PCM is binary mono signed little-endian PCM16. `asr.chunk_ms` bounds wire splitting and is capped at 100 ms; the adapter does not insert silent audio or drop supplied silence. It paces sends at approximately real time, rebasing after substantial lateness rather than replaying a large backlog as a burst. Input samples remain budgeted throughout writer ownership, conversion, and pacing. At most 64 command slots (or the smaller host limit) cover queued/writer-held audio and outstanding finalize markers. Pending markers retain their slots until acknowledgement. Finish is reserved independently of queue saturation. The event queue holds at most 16 transcript events, each limited by `max_transcript_bytes`; control and terminal storage are separate and bounded. WebSocket frames/messages are limited to 262144 bytes.

Confirmed transcript deltas append verbatim; mutable tails replace, never append. No delta trimming or inserted spaces occur inside the adapter. Full replacement Partial snapshots have connection-monotonic revisions, and one full confirmed Final is emitted per `flush_done`, including an empty boundary when necessary. `request_id` identifies the connection, not an utterance. Local utterance IDs advance only at boundaries; runtime remapping keeps identities monotonic across reconnects. Silence auto-finalization confirms prefix text without independently ending the runtime utterance. Already selected partial-based turns are not rewritten by a late Final.

**Integration assumption:** pending finalizes and transcript boundaries are attributed FIFO: all text for a flush must precede its acknowledgement, and successor text must follow it. PCM continues while acknowledgements are pending; there is no per-flush stop-and-wait pause. Official schema/SDK descriptions support the manual command flow but do not completely establish multiple-outstanding-finalize and silence-race semantics for Ink Whisper. Local tests verify adapter behavior under that ordering, not the real server guarantee. The ignored contract and live tests must validate it before relying on the combination. Wrong connection IDs, unsolicited acknowledgements, missing acknowledgements, malformed events, and budget failures terminate the stream; an indistinguishable duplicate acknowledgement or semantic misattribution cannot always be detected locally.

`finish_input()` drains admitted PCM and pending flushes, sends plain text `close`, and accepts only normal `done` as graceful completion. Unconfirmed tails never become Final. Its original deadline survives finished-input Drop, pings, output backpressure, and acknowledgement activity. A later error preserves already-admitted confirmed Finals before the error, while discarding obsolete queued partials; terminal failure never depends on free output capacity. Explicit cancellation, output/control Drop, legacy `close`, and runtime stop retain bounded abort semantics and do not promise graceful final transcripts. Reconnect is fresh, without PCM replay.

## Cartesia TTS

The full Cartesia sample selects both providers. `full-duplex-demo/configs/voice-runtime.cartesia-tts.yaml` remains available for typed input plus Cartesia TTS diagnosis. The keyless default sample remains inject ASR plus mock TTS.

Set all provider-specific common fields explicitly: `tts.provider: cartesia`, endpoint `wss://api.cartesia.ai/tts/websocket`, model `sonic-3.6`, `api_key_env: CARTESIA_API_KEY`, `voice_env: CARTESIA_VOICE_ID`, language `ko`, and rate `24000`. Changing just the provider name keeps the Qwen defaults and is not a supported migration. The factory accepts `sonic-3.6`, `sonic-3.5`, `sonic-3`, and `sonic-latest`; model aliases and account/voice availability still require paid validation. Session language accepts `ko`, `ko-KR`, and `Korean`, normalized to `ko`, or `en`, `en-US`, `en-GB`, and `English`, normalized to `en`. Other languages are rejected rather than silently falling back. This provider-level support does not adapt the demo agent prompts or turn-taking policy.

| Key | Default | Validation and use |
|---|---|---|
| `tts.cartesia.api_version` | `2026-08-14` | Currently the only supported schema version; sent as `Cartesia-Version`, not inferred from a test date |
| `tts.cartesia.max_buffer_delay_ms` | 0 | Integer 0–5000; sent explicitly and unchanged on every continuation and finish |

Cartesia settings and credentials are validated by its factory, not by generic YAML loading or unused provider strings on the host-injected path. Missing nested settings preserve old YAML compatibility. Endpoint queries and embedded credentials are rejected. Production endpoints must use WSS; local test servers may use WS on loopback. Voice is required at session open. Requested rates must be 8000, 16000, 22050, 24000, 44100, or 48000 Hz; output is raw mono `pcm_s16le`. `tts.chunk_ms` does not control Cartesia's server chunks: the adapter bounds and normalizes received chunks instead.

A response owns one socket and a unique context, independent text writing and audio reading, a 64-command budget including writer-held text, a 16-item normalized audio queue, and a separate terminal slot. Finish reserves no command-queue capacity: the writer drains accepted fragments before sending empty text with `continue:false`. Accepted text counts toward the cumulative byte budget; a fragment whose JSON representation cannot fit the wire budget is also rejected. WebSocket frames/messages are limited to 262144 bytes, decoded raw audio to 65536 bytes, and normalized audio to 8192 bytes. Socket writes are bounded at two seconds and the remaining response deadline. The absolute deadline starts at first successful nonempty admission, never resets on append, and applies even when output is unpolled or full. Successful finish followed by input Drop permits output drain; other cancellation/Drop paths close ownership. Late continuations are conservatively rejected after one second without received audio once audio has started; context expiry never creates a replacement context. This does not promise arbitrary host think-time.

The dedicated sample disables premade warmup and uses the reservation task spec. It requires `OPENAI_API_KEY` as well as Cartesia credentials; task-spec probe failure is strict, not a mock fallback. Enabling premade synthesis later can incur costs before browser input.

## Language scope

The project is a full-duplex voice runtime, not a single-language product. Its current defaults and demo are language-specific: the ASR locale is `ko-KR`, the agent prompts and deterministic speech cues use Korean, and runtime TTS requests use `tts.language` (default `Korean`). Changing only `asr.locale` does not adapt these other components. Using English or another language requires suitable provider models and voices, adapted prompts and policies, and verification of the TTS request language and transactional claim classification. These implementation defaults should not be confused with the project's purpose, and multilingual end-to-end behavior has not been validated.

## Duplex safety limits

| Key | Default | Enforcement |
|---|---:|---|
| `asr.open_timeout_ms` | 10000 | One connect/auth/readiness attempt; idle transcript silence is not a failure |
| `asr.write_timeout_ms` | 2000 | Audio and commit socket writes |
| `asr.input_buffer_ms` | 1000 | Pending mono PCM16 sample budget, `input_sample_rate_hz * input_buffer_ms / 1000`; commands are separately bounded at 64 |
| `tts.open_timeout_ms` | 10000 | Text-session open and built-in Qwen setup; a buffered bridge does not start its provider until finish |
| `tts.language` | `Korean` | Normal and premade synthesis requests, separately from `asr.locale` |
| `tts.request_timeout_ms` | 30000 | One response; buffered deadline starts at finish admission, native providers start at the first accepted nonempty text and must not extend it on append |
| `tts.max_input_bytes` | 65536 | Accumulated UTF-8 text bytes per response |
| `speech.playback_stop_ack_timeout_ms` | 1000 | After Abort, await interrupted ACK before conservative settlement; valid range 1–5000 |
| `speech.playback_ack_timeout_ms` | 15000 | Playback inactivity after first nonempty audio; provider chunks do not refresh this timer; zero disables this existing watchdog only |

New safety limits must be positive. The sample budget is checked for overflow and must fit a positive `u32`. Buffered input waiting for the host to finish is not synthesis activity and does not trigger a request timeout. Provider owners enforce deadlines independently of output polling. The built-in buffered bridge retains one terminal separately from its normalized 16-item audio queue, so timeout cleanup does not depend on queue space.

PCM output is mono s16le. A decoded raw provider chunk is limited to 65536 bytes and normalized into at most 8192-byte chunks with per-response sequences; zero rates, odd byte lengths, rate changes, and duplicate/reordered sequences are rejected. The preferred rate is advisory: the actual emitted rate is sent to the browser and used by the ledger. Premade clips are limited to 1 MiB each and cached only after valid nonempty audio and a successful terminal.

## Injected provider settings and migration

`VoiceRuntime::build_with_providers(config, SpeechProviders { asr, tts })` shares the built-in assembly but skips provider-name construction. Endpoint, model, and API-key environment settings are host-owned on this path. Common locale/manual-commit, language/voice, timing, premade, and safety budgets still apply to sessions. Factories must report their capabilities honestly; a serial-only ASR is rejected rather than disguised as duplex, and an `Incremental` TTS without text-stream support is rejected without opening a network connection. Legacy buffered TTS factories are adapted exactly once.

Task-spec probe failures are strict for injected providers, even if the config still says `mock`. For a custom keyless setup, omit `agents.task.spec`; the existing built-in keyless fallback remains unchanged. New config fields require updates to exhaustive Rust struct literals, although YAML omission remains compatible through serde defaults. Existing `TtsRequest`, `AsrSessionConfig`, `PlaybackSink::command`, and legacy provider trait methods remain usable. Internal voice events add reset/provenance/source information. `AsrEvent::InjectedFinal` identifies standalone duplex decorator injection independently of provider IDs; exhaustive event consumers need to handle that additive variant. These are not browser wire changes.

Together now uses the documented realtime endpoint and JSON/base64 messages rather than the previous unverified binary/session-create dialect. Explicit old endpoint overrides are not rewritten: update them to `wss://api.together.ai/v1/realtime`. The adapter waits for `session.created`, supports documented manual commit, and rejects rates other than 16 kHz. See the [official protocol](https://docs.together.ai/reference/audio-transcriptions-realtime); live model/locale compatibility still needs paid contract validation.

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

With `framework` enabled and `agents.task.spec` present, runtime startup probes the spec and credentials; actual `RuntimeAgent` construction through `AgentBuilder` occurs separately for each session. Built-in development setups (`mock`/`inject` ASR plus `mock` TTS) preserve the keyless `MockReservationAgent` fallback on probe failure. Live built-in providers and injected providers fail strictly. An injected task spec without the `framework` feature is rejected rather than silently selecting the mock agent; omit the spec for an intentionally keyless injected setup.

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
