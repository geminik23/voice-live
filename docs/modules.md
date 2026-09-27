# Module reference

Module layout of `.`. The `framework` feature (enabled by default) pulls in the optional crates.io `ai-agents` dependency at version `1.0.10`; building with `--no-default-features` leaves only the deterministic core for fast testing.

## Foundations

| Module | Description |
|---|---|
| `ids.rs` | `SessionId`, `TaskId`, `SpeechId`, `TurnId`, `Revision` newtypes |
| `clock.rs` | `MonotonicClock` trait. `TokioClock` uses Tokio time, so all `start_paused` virtual-time tests work. `ManualClock` is for pure unit tests |
| `meta.rs` | `EventMeta { monotonic_us, wall_clock }` + `MetaFactory` |
| `epochs.rs` | Four `Epochs` counters and invalidation operations |
| `events.rs` | All `VoiceEvent` variants and `SessionSummary` |
| `config.rs` | `VoiceRuntimeConfig` (`voice-runtime.yaml` schema, serde defaults for all fields, `validate()` rejecting “아니” as a hard stop, etc.) |
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
| `snapshot.rs` | `InteractionSnapshot` - read-only state passed to the brain |
| `brain.rs` | `InteractionBrain` trait, `NoopInteractionBrain` (default), `ScriptedInteractionBrain` (tests) |

## semantics/

| Module | Description |
|---|---|
| `cue.rs` | `CueExtractor`. Regex-based extraction of HardStop/Correction/Negation/EntityCandidate/IntentCandidate |
| `frame.rs` | `SemanticFrame`, `SemanticValue(status)`, slot operations, dependency fingerprint. `SlotOperation::Clear` actually removes the slot |
| `extractor.rs` | `SlotExtractor` trait + `HeuristicKoreanSlotExtractor`. Extracts date/time/party size and detects `X 말고 Y` corrections for **all three slots** (only a correction emits `Replace`, and only `Replace` invalidates dependent tasks). Values are compared as `Value` values: `to_string()` adds JSON quotes, so the strings would never match. Can be replaced by `RuntimeSlotExtractor` (LLM) when the framework is enabled |
| `partial_policy.rs` | Gate for partial controller calls. Urgent calls run immediately; semantic deltas have a minimum interval and a maximum flush interval. Injected clock |

## speech/

| Module | Description |
|---|---|
| `act.rs` | `SpeechAct`, `ClaimClass`, `Interruptibility`, `EvidenceRef`, priority constants. The abort path respects `Interruptibility::Never`, but hard stop takes precedence; `AfterClause` is already satisfied because acts are split by clause |
| `claim_gate.rs` | Evidence checks by class. Process checks existence per ref (distinguishing MissingEvidence) |
| `ledger.rs` | `AudibleLedger`. Records `Heard::{Full, Partial, None}` per clause. Interrupted clauses retain their original text, exposing only the heard portion through `heard_text()`. Keeps only the most recent `speech.audible_history_max_clauses` clauses (for self-echo detection and summaries; agent memory holds the long-term record). `render_heard_turn` renders a reply as text to retain in memory (`… [중단됨]`, etc.) |

## session/

| Module | Description |
|---|---|
| `view.rs` | Pure `SessionView` projection + `SessionEvidence` storage |
| `supervisor.rs` | The full `VoiceSession`. Run loop, control tick, barge-in, commit, tool evidence, speech queue, speculative work, deep work, and **turn memory reconciliation** (`Audible` once all reply clauses settle; `Discard` if stale, failed, or superseded). Assembled with a builder |
| `task_runner.rs` | Manages `ActiveTask` and consumes agent turns. Dropping the stream releases the framework root-turn gate |
| `tts_worker.rs` | Worker consuming SpeechAct→TTS + `TtsCancellationRegistry` |
| `deep_worker.rs` | `DeepWorker` trait, `DeepWorkRequest/Result`, `FakeDeepWorker` for scenarios. No automatic trigger; runs only via `VoiceSessionHandle::request_deep_work` (→ `VoiceEvent::DeepWorkRequested`) |

## agent/

| Module | Description |
|---|---|
| `mod.rs` | `CognitiveAgent`/`AgentTurnStream` traits - boundary separating the runtime from the framework. `AgentTurnRequest` separates `input` (only committed utterances) from `context: TurnContext` (turn environment for the system prompt). `resolve_turn(task_id, TurnResolution::{Audible, Discard})` reconciles turn memory |
| `mock.rs` | `FakeAgent` (scripted; `AgentRecorder` records received requests and resolutions), `MockReservationAgent` (demo mock, no memory) |
| `framework.rs` | `AiAgentsCognitiveAgent`. `with_turn_memory` serializes turns with host-owned memory, savepoints, and reconciliation; `new` leaves memory to the framework (for speculative work). `apply_resolution` (restore savepoint / replace that turn's reply text), `conversation_memory_for_spec` (honors YAML `memory.max_messages`), `VOICE_CONTEXT_KEY`, `VoiceAgentHooks`, `RuntimeInteractionBrain`, `RuntimeSlotExtractor`, `RuntimeDeepWorker`, `build_runtime_agent*` |

## asr/ tts/

| Module | Description |
|---|---|
| `asr/mod.rs` | `StreamingAsr`/`AsrSession` traits, `AsrEvent` |
| `asr/fake.rs` | `MockAsr`, `ScriptedAsrSession`. Parks when the script ends instead of returning `Ok(None)`, which means “session closed” |
| `asr/inject.rs` | `InjectionQueue` (per session) + `InjectableAsr` decorator. Adds a text-injection channel to any inner provider |
| `asr/together.rs` | Together realtime adapter. `ParseState` owns the session-monotonic revision and utterance ID. Wire format still needs validation by contract tests |
| `tts/mod.rs` | `StreamingTts`/`TtsStream`, `TtsRequest`, `TtsEvent` |
| `tts/fake.rs` | `FakeTts` (guarantees chunk→done→None termination) |
| `tts/premade.rs` | `PremadeTts` - cache of pre-synthesized phrases. Helps meet the backchannel latency target |
| `tts/qwen.rs` | DashScope realtime adapter. Cancels by closing the socket if `response.cancel` is unsupported |

## Remaining modules

| Module | Description |
|---|---|
| `playback.rs` | `PlaybackCommand`, `ClientPlaybackSink` (production), `SimulatedPlaybackSink` (simulates the actual playback ACK flow in virtual-time tests), `UnacknowledgedPlaybackSink` (for ACK timeout scenarios) |
| `tools/reservation.rs` | Shared in-memory reservation backend, pure-read `search_availability`, and `reserve_restaurant` framework tool enforcing confirmation + idempotency |
| `scenario.rs` | Scenario DSL runner. YAML → virtual-time execution → expectation evaluation |
| `runtime.rs` | `VoiceRuntime`. Provider assembly, `BrainProvider` (creates a brain per session), session lifecycle, media worker, playback bridge |
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
