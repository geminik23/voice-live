# Architecture

This document describes the runtime's actual architecture. It stands alone: the original design discussions are private, uncommitted documents, so all necessary information is here.

## Overall structure

The transport layer (axum WebSocket) belongs to the **demo**, not the library. The gateway connects the `ClientInput` / `ServerMessage` channels to the socket. The library's `runtime.rs` handles frame sequence validation, resampling, VAD, and the playback bridge.

The library and demo server target native execution on Linux, Windows, and macOS, and must not require a particular OS or shell. The demo browser currently handles microphone capture and physical audio output, so runtime build validation is distinct from browser and device validation. See [deployment](deployment.md) and [testing](testing.md#platform-validation) for platform-specific prerequisites and validation scope.

```text
Browser (web/index.html)
    │  WebSocket: binary PCM (actual browser rate, 20 ms frames) + JSON control
    ▼
Axum gateway (demo: src/gateway.rs)
    │  ClientInput channel
    ▼
Media worker (library: runtime.rs)
    │  validate frame sequence → resample → energy VAD → nonblocking ASR admission
    ▼
VoiceEvent bus (events.rs)
    │
    ├─▶ Session Supervisor (session/supervisor.rs)
    │      - 40 ms control tick (checks only deterministic timeouts)
    │      - transcript reconciler (revisioned partial)
    │      - reflex policy (Korean sentence endings, backchannel, hard stop)
    │      - call Interaction Brain (LLM) only when uncertain
    │      - SpeechAct priority queue + Claim Gate
    │
    ├─▶ Task Agent (ai-agents RuntimeAgent)
    │      - input only committed turns
    │      - tool lifecycle → collect evidence
    │      - speculative read / deep worker use separate runtime instances
    │
    └─▶ TTS worker (session/tts_worker.rs)
           - synthesize per SpeechAct
           - premade clip cache (pre-synthesized backchannels)
           - recover the queue with TtsFailed on failure
    │
    ▼
Playback bridge (library: runtime.rs → ServerMessage channel)
    │  speech_started JSON → binary chunks → speech_done JSON
    ▼
Axum gateway (demo: src/gateway.rs → WebSocket)
    ▼
Browser playback (24 kHz) → duck/resume/abort control + ACK
```

## Duplex speech providers

Speech providers are injectable: a host passes `SpeechProviders { asr, tts }` to `VoiceRuntime::build_with_providers` and keeps every endpoint, model, and API-key setting on its side. The built-in `build` path resolves the same assembly from the configured provider names, so both paths share the wiring.

STT input and transcript output are independent. The media worker runs the local VAD first and admits each frame to the provider with a nonblocking bounded queue, so a stalled network write can never delay barge-in detection, playback acknowledgement handling, or cancellation. The connection owner keeps a private generation counter: provider revisions and utterance ids restart with every connection, so the runtime re-issues both from session-lifetime counters, discards duplicate finals and late partials against a sealed high-water mark, and tags derived work (slot extraction, interaction decisions, speculative reads) with candidate provenance. Typed text injection is owned by the media session rather than any provider connection, so it keeps flowing through recognizer reconnects.

TTS text input and audio output are independent too. A provider declares `Buffered` or `Incremental` text input; buffered whole-text providers are bridged through `BufferedTtsAdapter`, which starts the inner request only once the input finishes. Input operations are nonblocking FIFO admissions; provider I/O runs independently. The runtime submits the complete authorized `SpeechAct` once and finishes the input, then drains audio with a request deadline. This does not turn agent provisional chunks into speech: general answers still follow authoritative `AgentFinal` → clauses → Claim Gate → speech. Qwen remains buffered; native incremental behavior is verified with deterministic fakes, not claimed for the live endpoint.

## Four time axes

| Loop | Timing | Responsibility | Implementation |
|---|---|---|---|
| Reflex | Immediately on event | VAD onset duck, hard stop abort | `session/supervisor.rs` |
| Interaction | 40 ms tick | Endpointing, overlap classification, backchannel | `session/supervisor.rs` + `interaction/` |
| Cognitive | Hundreds of ms to seconds | Task Agent root turn | `session/task_runner.rs` + `agent/` |
| Articulation | Per clause | SpeechAct queue + TTS | `speech/` + `session/tts_worker.rs` |

Audio frames never wait for an LLM or tool. Mic input, VAD, and playback run at a fixed rate regardless of LLM/tool latency.

## Event model and epochs

All asynchronous results flow through `VoiceEvent`, and the supervisor applies events to the `SessionView` projection. The projection is pure and performs no external I/O.

```text
Epochs { transcript, interaction, thought, speech }
```

- **transcript**: ASR partial revision. Decisions based on old partials are discarded. The adapter guarantees a revision that **increases monotonically per session** (synthesizing a counter if the provider does not supply one). If this value stops changing, both stale-result detection and the stable-prefix timer stop working.
- **interaction**: Invalidates Interaction Brain decisions. It increases whenever the floor actually changes (abort / resume / turn commit), so late LLM decisions based on an earlier floor are not applied.
- **thought**: Increases on corrections or the start of a new turn. Task results from earlier epochs are discarded.
- **speech**: Increases on abort or playback ACK timeout. TTS chunks from earlier epochs are dropped.

Logical invalidation ensures correctness even when physical HTTP cancellation is impossible.

## Three commit boundaries

1. **Transcript Commit** (`commit_user_turn`)
   Observed partial → stable prefix → semantic complete → `UserTurnCommitted`.
   Task Agent input begins here. Raw partials never enter durable memory.

2. **Action Commit** (`on_tool_completed`)
   `action_commit` evidence is created only when a `ToolEffect::TransactionalWrite` tool has both `ToolExecutionRecord.executed = true` and a successful result. Transactional speech cannot pass the Claim Gate without this evidence.

3. **Audible Commit** (`speech/ledger.rs` + agent memory)
   Playback ACKs establish whether each clause was heard (`Heard::Full / Partial / None`). Once all clauses in a reply have settled, the assistant message in that turn's agent memory is replaced with **what was actually heard**. The agent must not assume it delivered words the user did not hear. See “Conversation memory = what was heard” below.

## Conversation memory = what was heard

The ai-agents runtime stores a reply in memory **as soon as it generates it**, before playback and regardless of whether it was heard. voice-live owns the task agent's memory directly (`AgentBuilder::memory`) and reconciles each turn's record with the actual playback outcome.

### Two channels

| Data | Route | Stored in memory |
|---|---|---|
| Committed user utterance | User message (`AgentTurnRequest::input`) | Yes |
| Turn environment (`idempotency_key`, `semantic_frame`, speculative/deep results) | `context_manager().set("voice", …)` → system prompt `{{ context.voice.brief }}` | **No** |
| Assistant reply | Stored by the framework → reconciled by voice-live with what was heard | Yes (after reconciliation) |

The system prompt is rendered anew for each LLM call and is not stored as a message, so the turn environment cannot accumulate in memory. Previously, this environment and a JSON record of what was heard were placed in the user message; each retained user message repeated earlier snapshots, causing O(n²) token growth and leaving contradictory records.

### Turn lifecycle

```text
start_turn
  ├─ wait for the previous turn to resolve (adapter gate)
  ├─ savepoint = memory.snapshot()
  ├─ context_manager.set("voice", …)
  └─ chat_stream_events(transcript)
        … framework records the user message and generated reply in memory …

All clauses in the reply settle (played / interrupted / not played)
  └─ resolve_turn(Audible { final_text, heard })
       └─ wait for the framework stream to drop, then
          replace this turn's assistant message with heard

stale / failure / superseded by the next turn
  └─ resolve_turn(Discard)
       └─ memory.restore(savepoint)   — remove the entire turn
```

`heard` representation:

| Outcome | Assistant text retained in memory |
|---|---|
| Fully played | Original generated text unchanged |
| Interrupted mid-clause | Heard prefix + `… [중단됨]` |
| Later clauses not played | Earlier clauses + `[중단됨: 이후 내용은 전달되지 않음]` |
| Not delivered at all (TTS failure, claim rejection, etc.) | `[응답이 전달되지 않음]` |

Even if the claim gate rejects the entire reply, it is reconciled to `[응답이 전달되지 않음]`. An unverified “예약이 완료되었습니다” must not remain in memory as though it had been delivered to the user.

Partial hearing is an estimate. For a clause whose synthesis has finished, it uses the played-to-synthesized sample ratio; for one still being synthesized, it uses a **conservative speaking time per character**. If playback catches up with streaming synthesis, a simple ratio would say “fully heard,” precisely the error this safeguard is meant to prevent: recording words the user did not hear as heard.

### Safety rules

Memory edits do not acquire the framework's root-turn gate. Therefore:

1. **Never edit during an active turn.** The adapter serializes turns from admission through resolution with its own gate and edits only after the framework stream has dropped (the turn-finished signal).
2. **Use a single writer.** All resolutions run one at a time under the same gate.
3. **Do not split native tool exchanges.** Restore the exact previous snapshot, or replace only the assistant *text* for that turn. Find the replacement target by exact content match, searching backward and stopping at that turn's user message.
4. **Confirm turn completion with a signal.** If the consumer disappears, the forwarder promptly drops the framework stream and signals completion only after the drop.

To prevent a missing resolution from stalling the session, the next turn waits at most 10 seconds before leaving the previous turn “as generated” and proceeding.

### A new turn interrupts the previous reply

Once the user commits a new turn, the conversation has moved on. A previous turn still running inference is canceled and Discarded; interruptible playback, including ownerless phatic/process acts and already stopping speech, is aborted and conservatively settled using progress already confirmed at the commit boundary. Later ACKs cannot revise that boundary. The new turn's context must reflect what the user heard before speaking again.

`Never` protects the current act during setup, synthesis, and playback, including the period after synthesis finishes but before playback ACK. The runtime keeps one deferred committed turn with its selected semantic/speculative/deep context snapshot; a later committed turn supersedes that unstarted record without fabricating a merged user message or a framework resolution. Release order is protected speech settlement → owning previous reply resolution → consume the deferred record once → successor admission. Hard stop, provider failure, playback watchdog, and session teardown are safety termination paths and override Never.

A partially failed synthesis is never marked synthesis-complete or Full. It invalidates the speech epoch, sends Abort, discards the failed reply's queued clauses, and waits for interrupted ACK or a bounded stop deadline before another act may replace the single in-flight ledger. Duplicate or old terminals/ACKs are rejected before both projection and handling.

### Known limitation

When a state transition regenerates a reply, ai-agents (as tested with 1.0.11) **first** stores stale pre-transition text as an assistant message. Because there is no message-kind marker to distinguish it from a tool-call decision, that text remains. Only the final reply is reconciled. A framework `MessageKind` would resolve this.

## Claim Gate

Speech is classified into four classes and checked before TTS.

| Class | Required evidence | Example |
|---|---|---|
| Phatic | None | "네", "알겠습니다" |
| Process | ToolStarted | "조건에 맞는 곳을 확인하고 있어요" |
| Factual | AgentFinal or ToolCompleted(success) | "토요일 7시엔 두 곳이 가능합니다" |
| Transactional | ActionCommit(success) | "예약이 완료되었습니다" |

Rejected speech is recorded in the `voice_claim_rejected_total` metric and silently discarded.

## Barge-in: two-stage reversible duck

```text
VAD onset → immediate Duck (gain 0.15, fade 60ms, preserve buffer)
          → wait confirm_ms(240ms)
          → classify:
              backchannel word → ResumeAgent (discard partial)
              empty/echo       → ResumeAgent
              correction       → Abort + slot invalidation
              other            → Abort (SemanticInterruption)
hard stop phrase (e.g. "잠깐") → immediate Abort without classification + spoken "네, 멈췄어요." ACK
```

If the user finishes speaking during Duck, playback resumes; for a genuine interruption, the current clause is discarded. On abort, the hard stop ACK (`PRIORITY_HARD_STOP_ACK`) survives in the queue and plays after the user stops. Surviving acts are **retagged with the new speech epoch**; otherwise, the epoch filter discards them after they pass through the queue.

`Interruptibility` is interpreted on the abort path as follows:

| Value | Meaning |
|---|---|
| `Immediate` | Stop immediately |
| `AfterClause` | Already satisfied because acts are split by clause: do not start the next clause |
| `Never` | Let the currently playing act finish; clear only the queued acts behind it (`voice_abort_deferred_total`) |

However, **a hard stop takes precedence over `Never`**. Continuing to speak after the user explicitly says “그만” is a trap, not a feature.

Note: `"아니"` is deliberately excluded from the hard stop list. It is too common as a discourse marker and is classified as a correction cue; config validation (`VoiceRuntimeConfig::validate`) rejects its inclusion.

## Korean turn-taking

The deterministic reflex policy is primarily responsible for all decisions.

- **Incomplete ending** (ends as-is): `-는데/-고/-면/-다가/그게/...` → KeepListening even past soft silence (prevents premature commit)
- **Complete ending**: `-해 주세요/-할게요/-인가요?/-됩니다/-해줘`, etc. → commit at soft silence (500ms)
- **“네” after a question**: if `assistant_asked_question`, treat it as an answer and commit immediately at the minimum answer silence (320ms)
- **“네” during an explanation**: backchannel → duck, then resume; discard the partial
- Hard silence (1100ms) always commits as a safety net

`ReflexPolicy::classify_overlap` alone classifies overlap (backchannel / hard stop / correction / self-echo / unknown). `decide_on_tick` only turns that result into an action; it does not reclassify it.

### Proactive backchannel

When the assistant is silent, the user has **paused** (`floor == UserPausing`, mic closed), the candidate sentence ends incompletely, and silence is between the soft and hard thresholds, the assistant says “네” first. It is limited to `maximum_per_user_turn` times per turn with a `minimum_interval_ms` spacing.

Only uncertain states (between soft and hard thresholds with no clear ending) escalate to the Interaction Brain (LLM), which is rate-limited by `minimum_interval_ms` (250ms). Without an LLM (`NoopInteractionBrain`), the deterministic policy works on its own.

## Self-echo defense

Even with browser AEC, assistant TTS may re-enter the mic. `echo.rs` classifies an inbound partial as echo if its character-bigram Dice similarity to recently played text reaches `similarity_threshold` (0.82), and discards that partial from the commit path (without aborting, while retaining resume behavior).

## Abandoned ASR candidates

Connection resets clear only uncommitted hypotheses and work derived from them. Candidate provenance carries generation, source, and mapped utterance identity; audio and injection sources are explicit and do not rely on numeric ID ranges. Closing a candidate on its matching Final or partial-based user commit makes its selected writes authoritative, rejects late extraction, and protects promoted speculative work from later audio resets. An unrelated injected Final cannot close an audio candidate.

A bounded slot journal stores the original `Option<SemanticValue>` before candidate writes, including absence, confidence, status, and source revision. Repeated writes by the same candidate retain the baseline. Host writes retire the entry, so a later candidate write records the latest authoritative baseline. Rollback restores this metadata directly and advances frame revision without running correction policy or invalidating unrelated committed main turns. The journal does not undo tool side effects or revive cancelled tasks.

## Dependency-scoped invalidation

When a correction cue detects a slot replacement (`X 말고 Y`):

1. Update the slot in `SemanticFrame` (Revised state).
2. Cancel only active tasks dependent on that slot.
3. Increment the `thought` epoch.
4. Discard speculative results with overlapping dependencies.
5. Drop results from the previous epoch that arrive later.

Unrelated tasks, such as looking up the user's profile, remain active.

## Speculative read / Deep worker

- **Speculative read**: When intent confidence for a stable prefix exceeds the threshold, run a preliminary request in a dedicated YAML/RuntimeAgent instance that registers only pure-read tools (a separate instance is required because root turns are serialized). Store the result with a dependency fingerprint and include it in the main turn's context brief under `speculative_results:`.
- **Deep worker**: A separate runtime for long-running work. Apply `DeepWorkResult` only when its epoch matches, and include it in the main turn's context brief under `deep_worker_results:`.

  **The deep worker has no automatic trigger.** The agent stream has no signal that “this turn will take a long time,” so the host explicitly requests it through `VoiceSessionHandle::request_deep_work`. The session is responsible only for epoch scoping and result injection. The scenario DSL can drive this path with a `deep_work_requested` event.

Both features are disabled by default in config, and their behavior is regression-tested with scenarios.

## Session isolation

`RuntimeAgent` holds conversation memory and serializes root turns per runtime. Therefore, **create a new brain for each session** (`BrainProvider`). Sharing one would mix transcripts from different clients and cause concurrent sessions to block each other.

Only the tool backend (`ReservationStore`) is shared, as it models an external system. Text injection queues are also isolated per session.

## Failure recovery

| Situation | Behavior |
|---|---|
| ASR connection closes or errors | Emit `ProviderError`, reset the stream generation so work derived from abandoned partials is invalidated, then reconnect fresh with backoff from 250ms to 8s. Audio between connections is discarded; it is never replayed as recognized speech |
| ASR open rejected (auth or unsupported config) | Report and stop the ASR source; it is not retried until the next session start. Typed text injection keeps flowing |
| ASR input budget saturated | Report an overflow, drop the connection, and reconnect fresh; local VAD and control processing continue |
| TTS failure before any audio | Settle the act as undelivered, discard the failed reply's remaining clauses, and proceed to the next independent act |
| TTS failure after some audio | Abort the browser playback, settle with the interrupted acknowledgement bounded by `speech.playback_stop_ack_timeout_ms`, and settle with the confirmed progress when no acknowledgement arrives |
| Missing playback ACK | After `speech.playback_ack_timeout_ms` (default 15s) from the first real playback evidence, increment the speech epoch, send `Abort`, and release the queue. Merely clearing server state is insufficient: a slow client may still be playing, causing the next speech to overlap |
| Cannot resample client sample rate | Emit `ProviderError` (recoverable=false), then discard the audio. Do not send it to the provider at the wrong rate |

## Audio resampling

The browser may not honor the requested sample rate, so the client sends the **measured** `AudioContext.sampleRate` in `session_start`. The server selects a strategy with `media::build_resampler`.

- Integer ratio (48k → 16k): `DecimatingResampler` (windowed-sinc anti-alias)
- Otherwise (44.1k → 16k): `LinearResampler` (low-pass to the output Nyquist frequency, then linear interpolation)
- Failure: emit `ProviderError` rather than silently passing audio through

Decimation phase is computed from the **absolute stream index**. Using chunk-relative indices would cause phase jumps and clicks at every 20 ms boundary.

## Invariants

1. Handle partial transcripts only by revision.
2. Do not stack partial requests on the same RuntimeAgent.
3. Keep Transcript / Action / Audible commits separate.
4. Never speak results from a different epoch.
5. Never speak a claim without evidence.
6. Start barge-in with duck; abort only after confirmation.
7. `Ok(None)` from `AsrSession::next_event` means "closed," not "no event right now."
8. Do not share Task/Interaction brains across sessions.
9. Edit agent memory only between turns, after the framework stream has dropped.
10. A failed synthesis discards the rest of its reply; a settled speech record is never revived by a late acknowledgement.
11. The playback watchdog is armed by real playback evidence, never by synthesis progress.
