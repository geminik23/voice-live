# Module reference

Module layout of `.`. The `framework` feature (enabled by default) pulls in the optional crates.io `ai-agents` dependency at version `1.0.11`; building with `--no-default-features` leaves only the deterministic core for fast testing.

## Foundations

| Module | Description |
|---|---|
| `ids.rs` | `SessionId`, `TaskId`, `SpeechId`, `TurnId`, `Revision` newtypes |
| `clock.rs` | `MonotonicClock` trait. `TokioClock` uses Tokio time, so all `start_paused` virtual-time tests work. `ManualClock` is for pure unit tests |
| `meta.rs` | `EventMeta { monotonic_us, wall_clock }` + `MetaFactory` |
| `epochs.rs` | Four `Epochs` counters and invalidation operations |
| `events.rs` | All `VoiceEvent` variants and `SessionSummary` |
| `config.rs` | `VoiceRuntimeConfig` with neutral linguistic defaults, bounded rule/resource validation, explicit `SlotFallback`, and `TurnControlConfig::korean_example()` compatibility resources |
| `metrics.rs` | P50/P95/P99 snapshots of counters and latency series (up to 1024 samples) |
| `logging.rs` | JSONL event log (`target/voice-traces/<date>/<session>/events.jsonl`) |
| `echo.rs` | Text-similarity self-echo filter (bigram Dice) |
| `media.rs` | Audio frame codec, sequence tracker, and resampler. `build_resampler` chooses `DecimatingResampler` for integer ratios (windowed-sinc anti-alias; phase calculated from the absolute stream index), otherwise `LinearResampler` (low-pass then linear interpolation; carries over the last sample of the previous chunk to rebase phase, avoiding length-proportional drift). Failures are not silently ignored |
| `replay.rs` | PCM16 WAV source + realtime replay API using actual 20ms timestamps |
| `transcript.rs` | `TranscriptReconciler` (revisioned partials, stable-prefix computation), `TurnAssemblyBuffer` |
| `vad.rs` | Energy VAD. The floor adapts only while idle (speech does not raise the floor); onset/offset hysteresis |

## interaction/

| Module | Description |
|---|---|
| `state.rs` | `FloorState`, `TaskState`, `SpeechState`, `UserSpeechState`, `InteractionAction`, `InteractionDecision(Envelope)`, `InterruptionReason` |
| `policy.rs` | `ReflexPolicy`. Sentence-ending decisions, backchannel words, hard stops, overlap classification, and tick decisions. Punctuation-only patterns ("...") use raw tail matching (avoids empty-pattern matches). Overlap classification lives only in `classify_overlap`; `decide_on_tick` merely converts its result into an action |
| `snapshot.rs` | `InteractionSnapshot` - bounded current candidate, recent committed user turns, confirmed-heard assistant context, and session state passed to the brain |
| `brain.rs` | `InteractionBrain` trait with legacy decisions and optional semantic assessments, `NoopInteractionBrain` (default), `ScriptedInteractionBrain` (tests) |
| `semantic.rs` | Bounded, validated semantic assessment and slot updates; no playback or tool execution authority |

## semantics/

| Module | Description |
|---|---|
| `cue.rs` | `CueExtractor`. Application-owned literal hard stops and configured correction/negation/intent/entity regexes; no implicit language grammar |
| `text.rs` | Pure `TextAnalyzer`, `UserTextAnalysis`, bounded `TextRules`/ordered `QuestionRule`, and compiled `ConfiguredTextAnalyzer`; no clock, I/O, state mutation, or authorization |
| `frame.rs` | `SemanticFrame`, turn-local negation, `SemanticValue(status)`, slot operations, dependency fingerprint. `SlotOperation::Clear` actually removes the slot |
| `extractor.rs` | `SlotExtractor`, `NoopSlotExtractor`, explicit `configured_fallback`, and optional `HeuristicKoreanSlotExtractor` compatibility helper. Extracts date/time/party size and detects `X 말고 Y` corrections for **all three slots**. The supervisor invalidates dependencies when an accepted value changes, including Clear and absence-to-value; provisional cues do not cancel work. Values are compared as `Value` values. Can be replaced by `RuntimeSlotExtractor` (LLM) when the framework is enabled |
| `partial_policy.rs` | Gate for partial controller calls. Urgent calls run immediately; semantic deltas have a minimum interval and a maximum flush interval. Injected clock |

## speech/

| Module | Description |
|---|---|
| `act.rs` | `SpeechAct`, `ClaimClass`, `Interruptibility`, `EvidenceRef`, priority constants. The abort path respects `Interruptibility::Never`, but hard stop takes precedence; `AfterClause` is already satisfied because acts are split by clause |
| `claim_gate.rs` | Exact strict clause authorization binding plus legacy class checks; Process checks start-evidence existence, and queue/TTS admission rechecks expiry |
| `render.rs` | Strict/explicit legacy modes, bounded invocation observations, trusted `SpeechRenderer` host contract, and configured static prompt/tool-outcome resources with receipt and target checks |
| `ledger.rs` | `AudibleLedger`. Records `Heard::{Full, Partial, None}` per clause. Interrupted clauses retain their original text, exposing only the heard portion through `heard_text()`. Keeps only the most recent `speech.audible_history_max_clauses` clauses (for self-echo detection and summaries; agent memory holds the long-term record). `render_heard_turn` renders a reply as text to retain in memory (`… [중단됨]`, etc.) |

## session/

| Module | Description |
|---|---|
| `view.rs` | Pure `SessionView` projection + `SessionEvidence` storage |
| `supervisor.rs` | Event orchestration with consumed-audio and speech admission, bounded semantic and compatibility extraction, candidate provenance/journal rollback, dependency-scoped work invalidation, deep-task admission, playback recovery, and worker-finished turn resolution |
| `task_runner.rs` | Manages `ActiveTask` and consumes agent turns. Dropping the stream releases the framework root-turn gate |
| `tts_worker.rs` | Complete authorized act → cache hook or duplex text admission/audio drain, request/open deadlines, response/format/sequence validation, pre-admission provider error preservation, and cancellation registry |
| `deep_worker.rs` | `DeepWorker` trait, `DeepWorkRequest/Result`, `FakeDeepWorker` for scenarios. No automatic trigger; `VoiceSessionHandle::request_deep_work` sends `DeepWorkRequested`. Results must match a pending task and its original epoch, and use the request's dependencies |

## agent/

| Module | Description |
|---|---|
| `mod.rs` | `CognitiveAgent`/`AgentTurnStream` traits - boundary separating the runtime from the framework. `AgentTurnRequest` separates `input` (only committed utterances) from `context: TurnContext` (turn environment for the system prompt). `resolve_turn(task_id, TurnResolution::{Audible, Discard})` reconciles turn memory |
| `mock.rs` | `FakeAgent` (scripted; `AgentRecorder` records received requests and resolutions), `MockReservationAgent` (explicit Korean compatibility helper, no implicit selection) |
| `proof.rs` | Framework-only `VoiceAgentHooks` and bound host tools; immutable SDK execution-scope origin, retry-safe turn tokens, bounded terminal proof projection, and no current-turn reassignment; canonical terminal retirement is independent of optional SDK stream chunks |
| `framework.rs` | `AiAgentsCognitiveAgent`. `with_turn_memory` serializes turns with host-owned memory, savepoints, and reconciliation; `new` leaves memory to the framework (for speculative work). `apply_resolution` (restore savepoint / replace that turn's reply text), `conversation_memory_for_spec` (honors YAML `memory.max_messages`), `VOICE_CONTEXT_KEY`, `VoiceAgentHooks`, `RuntimeInteractionBrain` (legacy), stateless `RuntimeSemanticBrain`, `RuntimeSlotExtractor`, `RuntimeDeepWorker`, `build_runtime_agent*`. The slot extractor rejects a whole malformed response or a response over 16384 bytes, limits updates to 32 and slot names to 128 bytes, accepts only `set`, `replace`, and `clear`, and checks confidence in 0–1. As before, `old_value: null` is represented as a present JSON null |

## asr/ tts/

| Module | Description |
|---|---|
| `asr/mod.rs` | Legacy `open`/`AsrSession` plus additive `supports_duplex`/`open_duplex`, `AsrAudioInput`, cancel-safe `AsrEventStream`, typed admission/open errors, and `LegacyDuplexFacade` |
| `asr/fake.rs` | Duplex `MockAsr`, PCM recorder, persistent virtual-time event deadlines, and legacy façade; quiet input parks until cancelled instead of reporting EOF |
| `asr/inject.rs` | `InjectionQueue` (per session) + `InjectableAsr` decorator. Adds a text-injection channel to any inner provider |
| `asr/together.rs` | Together realtime adapter. `ParseState` owns the session-monotonic revision and utterance ID. Wire format still needs validation by contract tests |
| `asr/cartesia.rs` | `CartesiaStreamingAsr::from_config` or host-owned `with_api_key`; Ink Whisper manual STT with independent paced raw PCM input and revisioned replacement output, verbatim delta accumulation, bounded pending FIFO flushes, absolute finalize/finish deadlines, confirmed-Final retention, terminal ownership, and native legacy facade. FIFO vendor flush attribution remains an explicitly unverified integration assumption |
| `tts/mod.rs` | Legacy `synthesize`/`TtsStream`, additive `TextInputMode`, text-session input/output/control handles, capability/cache hooks, bounded PCM normalization, and `synthesize_via_text_stream` retaining native control ownership |
| `tts/buffered.rs` | `BufferedTtsAdapter`: bounded text accumulation, finish-triggered synthesis, owner-enforced request deadline, normalized audio queue and separate terminal slot |
| `tts/fake.rs` | `FakeTts` (guarantees chunk→done→None termination) |
| `tts/premade.rs` | Success-only premade cache with matching text/language/voice and fixed factory settings; bounded warmup, complete-request cache hook, native capability forwarding, cancellable replay |
| `tts/qwen.rs` | DashScope realtime adapter. Cancels by closing the socket if `response.cancel` is unsupported |
| `tts/cartesia.rs` | `CartesiaStreamingTts::from_config` or host-owned `with_api_key`; native incremental Sonic WebSocket input, context-scoped raw PCM normalization, bounded I/O, absolute response deadline, separate terminal slot, typed/redacted errors, and owned legacy `synthesize` convenience. Syntax-bounded full region/script selectors and historical aliases; account/model/voice combinations still require validation |

## Remaining modules

| Module | Description |
|---|---|
| `provider.rs` | `ProviderSessionControl` cancellation/close contract and private owned worker cleanup; custom providers must enforce Drop and bounded teardown themselves |
| `playback.rs` | `PlaybackCommand`, `ClientPlaybackSink` (runtime nonblocking overload shutdown or legacy standalone admission), `SimulatedPlaybackSink` (simulates the actual playback ACK flow in virtual-time tests), `UnacknowledgedPlaybackSink` (for ACK timeout scenarios) |
| `tools/reservation.rs` | Explicit reservation example: shared in-memory backend, `search_availability` with `searched` receipts, and confirmed/idempotent `reserve_restaurant` with `committed` receipts; not a core speech-meaning registry |
| `scenario.rs` | Scenario DSL runner. YAML → virtual-time execution → expectation evaluation |
| `runtime.rs` | `VoiceRuntime`, `SpeechProviders` injection and capability assembly, `RuntimePolicies`/`build_with_policies` host overrides and per-session interaction/slot factories, per-session brains, owned supervisor/media/playback workers, nonblocking VAD/audio admission, fresh ASR retry and identity mapping, one-second terminal drain after input closure to preserve fatal classification, bounded client output |
| `protocol.rs` | `ClientControlMessage` / `ServerMessage`. Defines **what** is exchanged, not **how** it is transported |

## Per-session vs. shared

| Resource | Scope | Reason |
|---|---|---|
| `StreamingAsr` / `StreamingTts` | Shared | Only factories; each session receives its own stream through `open`/`synthesize` |
| `RuntimeAgent` (task/speculative/interaction/deep) | **Per session** | Holds conversation memory and serializes root turns per runtime |
| Task agent conversation memory | **Per session, owned by voice-live** | To reconcile replies with what was heard and roll back turns |
| `InjectionQueue` | **Per session** | One client's input must not become another client's transcript |
| `ReservationStore` | Shared | Models an external system (reservation backend) |
| `Metrics` | Shared | Process-wide observability |

## Transport boundary

The library has no web framework dependency. The runtime exposes only a pair of channels; the host connects them to a socket.

```text
  voice-live (library)                full-duplex-demo (host)
  ─────────────────────               ─────────────────────────
  VoiceRuntime::create_session(
      client_tx: Sender<ServerMessage>  ◄── demo writes to WebSocket
  ) -> GatewaySession {
      input_tx: Sender<ClientInput>     ◄── demo reads from WebSocket
  }

  protocol.rs  message types only      src/gateway.rs  axum / ServeDir
  media.rs     binary frame format
```

Switching to WebRTC requires replacing only `full-duplex-demo/src/gateway.rs`.
**Adding a web dependency to the root `Cargo.toml` is an architectural regression.**
