# Architecture

This document describes the runtime's actual architecture and current contracts.

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
    │      - immediate VAD/stop and deterministic timing
    │      - bounded semantic worker or explicit compatibility policy
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

STT input and transcript output are independent. With manual commit, the media worker sends the provider commit `asr.commit_delay_ms` of audio after local speech ends rather than at the end itself, and cancels it if speech resumes, so a short pause stays in one utterance; a pending commit never crosses into a new connection generation. The media worker runs the local VAD first and admits each frame to the provider with a nonblocking bounded queue, so a stalled network write can never delay barge-in detection, playback acknowledgement handling, or cancellation. The connection owner keeps a private generation counter: provider revisions and utterance ids restart with every connection, so the runtime re-issues both from session-lifetime counters, discards duplicate finals and late partials against a sealed high-water mark, and tags derived work (slot extraction, interaction decisions, speculative reads) with candidate provenance. Typed text injection is owned by the media session rather than any provider connection, so it keeps flowing through recognizer reconnects.

TTS text input and audio output are independent too. A provider declares `Buffered` or `Incremental` text input; buffered whole-text providers are bridged through `BufferedTtsAdapter`, which starts the inner request only once the input finishes. Input operations are nonblocking FIFO admissions; provider I/O runs independently. The runtime submits the complete authorized `SpeechAct` once and finishes the input, then drains audio with a request deadline. This does not turn agent provisional chunks into speech: general answers still follow authoritative `AgentFinal` → clauses → Claim Gate → speech. Qwen remains buffered. Cartesia TTS implements native incremental input with a socket/context per response; offline socket tests verify its wire and lifecycle behavior, not paid endpoint suitability. Cartesia STT implements independent manual Ink Whisper PCM input and transcript output; its ordering assumptions remain subject to paid contract verification.

### Cartesia manual transcript boundaries

The STT adapter appends finalized segment deltas verbatim to a confirmed prefix and replaces a mutable tail. It publishes changed full replacement Partial snapshots, not a Final for each segment. Explicit finalize commands are pipelined with continuous real-time PCM, and a bounded pending FIFO retains command permits until `flush_done`. Each acknowledgement emits one confirmed utterance Final before successor Partial delivery. Silence-based segment finalization alone does not end an utterance. Connection `request_id` is validated separately from adapter-local utterance IDs, which the runtime remaps across connection generations.

This mapping assumes FIFO flush attribution: pre-finalize text is emitted before its acknowledgement and successor text after it. Schema/SDK command descriptions and examples are not a complete Ink Whisper server ordering guarantee, especially with silence auto-finalization and several outstanding flushes. Local fakes test this explicit contract; they do not prove the vendor behavior. Detectable protocol/identity/deadline violations fail the connection, but indistinguishable duplicate acknowledgements and semantic cross-boundary text cannot always be identified. Live validation remains required, not silently replaced by stop-and-wait, batch STT, or another model.

The STT owner supervises each flush's write/ack deadline and an absolute finish deadline beginning at finish admission. Graceful finish drains queued PCM and pending flushes, sends plain text `close`, and finalizes only confirmed prefix text on normal `done`. Input Drop after accepted finish does not cancel that drain or its deadline. Pings are serviced during pacing, and a bounded separate terminal survives unpolled/full output. On failure, queued confirmed Finals are retained before the error; obsolete partials and unconfirmed tails are not promoted. Runtime stop remains a bounded abort rather than a promise of graceful transcript drain.

### Provider terminal ownership

Cartesia TTS has independent writer and reader futures under an owner-enforced absolute response deadline. A separate terminal slot survives a full normalized audio queue. Success requires the final `continue:false` write to complete; merely admitting local finish cannot authorize a provider terminal while accepted text is still queued. Context-less errors remain failures, irrespective of a missing or true `done` flag. Runtime pre-admission checks preserve provider error reasons and still reject unexpected pre-input audio. Input Drop after successful finish allows drain; output/control Drop, cancellation, and session stop retain bounded abort semantics. No provider cancellation guarantee replaces speech epochs, browser Abort, or heard-only ledger settlement.

If ASR input closes before its queued terminal is consumed, the media worker enters a bounded one-second terminal-drain state instead of immediately treating it as a retryable disconnect. It stops admitting PCM to that connection while local VAD, controls, and typed injection continue. A fatal terminal stops ASR; a recoverable terminal, EOF, or missing terminal at the absolute deadline reconnects fresh without replay. Additional events do not extend the drain deadline.

## Four time axes

| Loop | Timing | Responsibility | Implementation |
|---|---|---|---|
| Reflex | Immediately on event | VAD onset duck, hard stop abort | `session/supervisor.rs` |
| Interaction | 40 ms tick | Endpointing, overlap classification, backchannel | `session/supervisor.rs` + `interaction/` |
| Cognitive | Hundreds of ms to seconds | Task Agent root turn | `session/task_runner.rs` + `agent/` |
| Articulation | Per clause | SpeechAct queue + TTS | `speech/` + `session/tts_worker.rs` |

Audio frames never wait for an LLM or tool. Mic input, VAD, and playback run at a fixed rate regardless of LLM/tool latency.

## Event model and epochs

Provider, task, and playback events flow through `VoiceEvent`; the bounded semantic worker returns through private session channels. The supervisor checks provenance before applying a result. `SessionView::apply` is a pure projection and performs no external I/O.

```text
Epochs { transcript, interaction, thought, speech }
```

- **transcript**: ASR partial revision. Decisions based on old partials are discarded. The adapter guarantees a revision that **increases monotonically per session** (synthesizing a counter if the provider does not supply one). If this value stops changing, both stale-result detection and the stable-prefix timer stop working.
- **interaction**: Invalidates Interaction Brain decisions. It increases whenever the floor actually changes (abort / resume / turn commit), so late LLM decisions based on an earlier floor are not applied.
- **thought**: Increases when accepted slot values change or a new main turn starts. Each task retains its original epoch and identity. A slot change retires only tasks with matching dependencies, so an unaffected active task may still return its original epoch; a new main turn retires the previous main and outstanding deep work.
- **speech**: Increases on abort or playback ACK timeout. TTS chunks from earlier epochs are dropped.

An interaction decision cannot commit or take the floor while local VAD is active. The semantic worker checks candidate identity, transcript content, speech activity, interaction/thought/speech epochs, and frame revision before applying results. It allows an identical-text revision to remain valid, but rejects a changed candidate or speech boundary. Legacy decision workers carry a separate activity marker so a result from before speech ends is rejected.

Logical invalidation ensures correctness even when physical HTTP cancellation is impossible, but only for the boundaries that actually advance the relevant epoch.

## Three commit boundaries

1. **Transcript Commit** (`commit_user_turn`)
   Observed partial → stable prefix → semantic complete → `UserTurnCommitted`.
   Task Agent input begins here. Raw partials never enter durable memory.

2. **Action Commit** (`on_tool_completed`)
   Strict speech uses invocation-scoped executor observations plus application validation of the exact business receipt and target. `executed && success` is insufficient. The `action_commit` projection from executed/successful tool events remains as session evidence and the `voice_action_commit_total` metric; the runtime's own replies no longer use it as authorization.

3. **Audible Commit** (`speech/ledger.rs` + agent memory)
   Playback ACKs establish whether each clause was heard (`Heard::Full / Partial / None`). Once all clauses in a reply have settled, the assistant message in that turn's agent memory is replaced with **what was actually heard**. The agent must not assume it delivered words the user did not hear. See “Conversation memory = what was heard” below.

## Conversation memory = what was heard

The ai-agents runtime stores a reply in memory **as soon as it generates it**, before playback and regardless of whether it was heard. voice-live owns the task agent's memory directly (`AgentBuilder::memory`) and reconciles each turn's record with the actual playback outcome.

### Two channels

| Data | Route | Stored in memory |
|---|---|---|
| Committed user utterance | User message (`AgentTurnRequest::input`) | Yes |
| Turn environment (`idempotency_key`, `speech_language`, `semantic_frame`, speculative/deep results) | `context_manager().set("voice", …)` → system prompt `{{ context.voice.brief }}` | **No** |
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
| Fully played | Actual rendered speech; unchanged only when it equals the original Final |
| Interrupted mid-clause | Heard prefix + `… [voice-live:interrupted]` |
| Later clauses not played | Earlier clauses + `[voice-live:rest-undelivered]` |
| Not delivered at all (TTS failure, claim rejection, etc.) | `[voice-live:undelivered]` |

The markers are fixed tokens for the model, not phrases in any conversation language, and are never spoken. Agent instructions explain them (the demo task spec does). Even if the claim gate rejects the entire reply, it is reconciled to `[voice-live:undelivered]`. An unverified success claim must not remain in memory as though it had been delivered to the user.

Partial hearing is an estimate. For a clause whose synthesis has finished, it uses the played-to-synthesized sample ratio; for one still being synthesized, it uses a **conservative speaking time per character**. If playback catches up with streaming synthesis, a simple ratio would say “fully heard,” precisely the error this safeguard is meant to prevent: recording words the user did not hear as heard.

### Safety rules

Memory edits do not acquire the framework's root-turn gate. Therefore:

1. **Never edit during an active turn.** The adapter serializes turns from admission through resolution with its own gate and edits only after the framework stream has dropped (the turn-finished signal).
2. **Use a single writer.** All resolutions run one at a time under the same gate.
3. **Do not split native tool exchanges.** Restore the exact previous snapshot, or replace only the assistant *text* for that turn. Find the replacement target by exact content match, searching backward and stopping at that turn's user message.
4. **Confirm turn completion with a signal.** If the consumer disappears, the forwarder promptly drops the framework stream and signals completion only after the drop.

The supervisor reports a resolution only after its main worker has finished the `start_turn` path, so cancellation cannot resolve before the adapter has registered its savepoint. To prevent a missing resolution from stalling the session, the next turn waits at most 10 seconds before leaving the previous turn “as generated” and proceeding.

### A new turn interrupts the previous reply

Once the user commits a new turn, the conversation has moved on. A previous turn still running inference is canceled and Discarded; interruptible playback, including ownerless phatic/process acts and already stopping speech, is aborted and conservatively settled using progress already confirmed at the commit boundary. Later ACKs cannot revise that boundary. The new turn's context must reflect what the user heard before speaking again.

`Never` protects the current act during setup, synthesis, and playback, including the period after synthesis finishes but before playback ACK. The runtime keeps one deferred committed turn with its selected semantic/speculative/deep context snapshot; a later committed turn supersedes that unstarted record without fabricating a merged user message or a framework resolution. Release order is protected speech settlement → owning previous reply resolution → consume the deferred record once → successor admission. Hard stop, provider failure, playback watchdog, and session teardown are safety termination paths and override Never.

A partially failed synthesis is never marked synthesis-complete or Full. It invalidates the speech epoch, sends Abort, discards the failed reply's queued clauses, and waits for interrupted ACK or a bounded stop deadline before another act may replace the single in-flight ledger. Duplicate or old terminals/ACKs are rejected before both projection and handling.

### Known limitation

When a state transition regenerates a reply, ai-agents (as tested with 1.0.11) **first** stores stale pre-transition text as an assistant message. Because there is no message-kind marker to distinguish it from a tool-call decision, that text remains. Only the final reply is reconciled. A framework `MessageKind` would resolve this.

## Claim Gate

The reply path is Final → write check → host renderer → exact authorized clause → Claim Gate → TTS. The model writes the reply text in the session's speech language; the runtime neither supplies wording nor detects language.

A strict Final is `{"text": …, "outcome": …, "question": …}`, or plain text, which is read as `text` alone:

- **No outcome.** The text is spoken as general (phatic) speech. It carries no business evidence, and the runtime does not check what it says.
- **Named outcome.** `speech.outcomes.<name>` names an operation, a claim class, receipt fields, and target bindings. Every record of that operation executed in this turn must be usable, carry the receipt fields, and match its executed arguments; otherwise nothing of the reply is spoken. Records are found by operation from the executor's own observations. The model never supplies or repeats a call ID, arguments, or evidence reference.
- **Unconfirmed write.** If a tool that may write (declared in `tools.effects` with an effect other than `pure_read`, or undeclared but actually run in this turn) was started and not completed, completed unsuccessfully, or left an unusable record in this turn, none of the reply is spoken, whatever it names. This check does not read the reply text, so it works in any language.
- **Rejection.** A malformed or unconfirmed reply is not spoken. The user hears `speech.failure_text` instead (when configured), and memory records the reply as `[voice-live:undelivered]` while keeping the turn's tool history.
- **Question.** The optional `question` value is only a turn-taking hint, never evidence. It reaches interaction state only after the clause has played in full.

The residual risk is explicit: a reply that names no outcome can still claim, in words, something that never happened (for example a reservation that was never attempted). The runtime blocks such claims only in a turn where a write failed. Arbitrary natural-language truth is not proven. A custom host `SpeechRenderer` can apply stricter domain rules. The original model Final remains the exact memory lookup identity even when only part of the text is heard.

Each rendered clause receives an immutable text/class/speech-ID binding in session evidence. Queue admission and TTS admission both check the binding and expiry. Deliberately surviving queued acts migrate their speech epoch together with that authorization during recovery; changing their meaning or reusing a different act ID cannot reuse the binding. Question hints become interaction state only after confirmed full playback of the associated clause, not after generation or authorization. Interrupted or rejected questions do not publish hints; a committed user turn clears prior question state.

The following class table describes the gate's evidence checks for acts that carry no renderer binding: runtime-owned phatic/process acts and acts a host builds itself. Reply clauses with a factual or transactional class instead require the validated renderer binding.

| Class | Required evidence | Example |
|---|---|---|
| Phatic | None | "네", "알겠습니다" |
| Process | ToolStarted | "조건에 맞는 곳을 확인하고 있어요" |
| Factual | AgentFinal or ToolCompleted(success) | "토요일 7시엔 두 곳이 가능합니다" |
| Transactional | ActionCommit(success) | "예약이 완료되었습니다" |

Rejected speech is recorded in the `voice_claim_rejected_total` metric and settled as undelivered. Reply text that names no outcome is admitted as phatic speech; it cannot carry a factual or transactional class without a confirmed outcome. The runtime has no phrase-based success detector for any language. Missing automatic text suppresses only the act, not hard-stop Abort, failure resolution, or queue recovery. A queued process act is retired on tool completion and when its task ends (reply, failure, or cancellation), so a progress line never plays after the reply or failure text; it also rechecks start evidence and expiry before TTS.

## Barge-in: two-stage reversible duck

```text
VAD onset → immediate Duck (gain 0.15, fade 60ms, preserve buffer)
          → wait confirm_ms(240ms)
          → classify:
              backchannel      → ResumeAgent (discard partial)
              empty/echo       → ResumeAgent
              correction       → Abort + applicable slot updates
              other            → Abort (SemanticInterruption)
explicit hard stop → immediate Abort; speak an ACK only when configured;
                     the stop words are consumed and never become a task turn
```

A classified backchannel or empty overlap resumes playback; a genuine interruption aborts the current clause. An unknown overlap is resolved conservatively after the configured wait rather than automatically resuming merely because speech ended. On abort, the hard stop ACK (`PRIORITY_HARD_STOP_ACK`) survives in the queue and plays after the user stops. Surviving acts are **retagged with the new speech epoch**; otherwise, the epoch filter discards them after they pass through the queue.

`Interruptibility` is interpreted on the abort path as follows:

| Value | Meaning |
|---|---|
| `Immediate` | Stop immediately |
| `AfterClause` | Already satisfied because acts are split by clause: do not start the next clause |
| `Never` | Let the currently playing act finish; clear only the queued acts behind it (`voice_abort_deferred_total`) |

However, **a hard stop takes precedence over `Never`**. Continuing to speak after the user explicitly says “그만” is a trap, not a feature.

The sample does not install language-specific hard-stop words. An explicitly configured compatibility analyzer may still use application-owned vocabulary; core validation does not prohibit a particular language's word.

## Neutral control and the explicit Korean example

With semantic interaction enabled, one asynchronous request interprets a bounded current candidate and confirmed-heard context. At most one request is in flight; changes are coalesced into the latest next request. The session retains immediate VAD duck, explicit stop, playback, and hard-silence control. A recognized stop is delivered independently of a slow host slot extractor. During overlap the session waits for a bounded semantic result, resumes for backchannels, and otherwise aborts conservatively after at most two configured interaction timeouts. Empty overlap resumes without waiting for a model. If the model is unavailable or its response is invalid, no language heuristic is silently installed.

An explicit host `TextAnalyzer` or legacy decision brain selects the compatibility path. There, the synchronous analyzer supplies advisory cues while the deterministic reflex policy owns timing and state decisions. Question hints come only from a reply's `question` field. The following examples describe explicitly selected Korean compatibility resources, not the shipped semantic sample or library defaults.

- **Incomplete ending** (ends as-is): `-는데/-고/-면/-다가/그게/...` → KeepListening even past soft silence (prevents premature commit)
- **Complete ending**: `-해 주세요/-할게요/-인가요?/-됩니다/-해줘`, etc. → commit at soft silence (500ms)
- **“네” after a question**: if `assistant_asked_question`, treat it as an answer and commit immediately at the minimum answer silence (320ms)
- **“네” during an explanation**: backchannel → duck, then resume; discard the partial
- Hard silence (1100ms) always commits as a safety net

`ReflexPolicy::classify_overlap` alone classifies overlap (backchannel / hard stop / correction / self-echo / unknown). `decide_on_tick` only turns that result into an action; it does not reclassify it.

### Invocation proof ownership

The framework adapter supplies a fresh turn-scoped sender token using the published SDK actor-context API, while retaining a stable effective actor identity per adapter/session. `VoiceAgentHooks::bind_tool` reads the immutable sender from the executor context; it never infers turn ownership from arguments or the currently active turn when a record arrives. Built-in registered tools are wrapped automatically. Hosts constructing their own framework agent must bind their tools explicitly; unbound/MCP or otherwise uncorrelated outcomes remain unproven.

Retries and approval-modified arguments retain the admitting turn token. The terminal record supplies canonical operation, logical call ID, source/fallback provenance, actual executed arguments, and outcome flags. Each observation receives its own record ID, so fallback records do not overwrite an original record with the same call ID. Late records for a released origin are dropped rather than reassigned. Tokens and execution keys are retired when the stream finishes/drops, including failure paths. Keys remain until that boundary so repeated terminal observations retain their origin and can be rejected as ambiguous instead of overwriting an earlier result. Model-facing Skip recovery without a matching complete proof cannot authorize a success claim.

The wrapper registers a bounded logical execution key (call ID, canonical operation, source, and executor start identity) under that token before invoking the backend. Retries reuse the request scope; terminal records, including timeout and Skip recovery, resolve that key rather than inferring origin from arguments or optional stream chunks. Canonical terminal events retire queued process speech even when SDK tool-event streaming is disabled or the requested name is an alias.

At most 128 origins and 128 total execution keys are retained per adapter, and 128 records per main turn; speculative records are not admitted to the speech-proof store. Argument and output projections are limited to 16384 bytes each. Identity fields are bounded before event emission or storage (record ID 128 bytes, call/operation IDs 256 bytes, serialized source 1024 bytes). Oversized identity observations produce a bounded invalid marker that invalidates the turn's proof set rather than retaining raw payloads or silently losing conflicts. Oversized/truncated result projections remain unproven. Proof payloads are not included in event logs. The session validates origin/task/epoch before storage and retires snapshots with the owning main turn. The configured renderer requires an unambiguous matching operation/call record, matching executed arguments, explicit business receipt checks, and target bindings. Host renderer code is trusted and must enforce equivalent domain semantics.

Cancellation, timeout, or stream Drop may happen after an external effect but before its receipt is observed. Missing proof means unknown/unproven, not nonexecution or backend rollback. Conversation savepoint rollback does not undo an external effect and is not evidence that retry is safe. No automatic recovery/re-execution is added.

### Proactive backchannel

When the assistant is silent, the user has **paused** (`floor == UserPausing`, mic closed), the candidate sentence ends incompletely, and silence is between the soft and hard thresholds, the assistant says “네” first. It is limited to `maximum_per_user_turn` times per turn with a `minimum_interval_ms` spacing.

Semantic requests are triggered by meaningful candidate or context changes, including final and injected text, not by every audio frame or control tick. `minimum_interval_ms` limits request starts; `timeout_ms` bounds each request; a timed-out request counts in `voice_semantic_timeout_total`, and the first timeout and the first provider failure of a streak are logged. Without an available semantic model, the configured semantic path retains neutral timing and does not fall back to the Korean example.

## Self-echo defense

Even with browser AEC, assistant TTS may re-enter the mic. `echo.rs` classifies an inbound partial as echo if its character-bigram Dice similarity to recently played text reaches `similarity_threshold` (0.82), and discards that partial from the commit path (without aborting, while retaining resume behavior).

## Abandoned ASR candidates

A separate constant-space consumed-audio watermark advances only when a nonempty current audio partial is actually sealed into a committed user turn. Audio Partial/Final events at or below that session-monotonic identity are rejected before projection and handling, so late provider results cannot repeat the turn or erase its successor. An empty successor after Final-first commit is not consumed. Injection finals are not filtered by this audio guard; an injection-only turn does not consume an audio ID. The boundary survives reconnects because runtime-issued audio identities never restart. A selected transcript is not implicitly rewritten by a later provider Final.

Connection resets clear only uncommitted hypotheses and work derived from them. Candidate provenance carries generation, source, and mapped utterance identity; audio and injection sources are explicit and do not rely on numeric ID ranges. Closing a candidate on its matching Final or partial-based user commit makes its selected writes authoritative, rejects late extraction, and protects promoted speculative work from later audio resets. An unrelated injected Final cannot close an audio candidate.

A bounded slot journal stores the original `Option<SemanticValue>` before candidate writes, including absence, confidence, status, and source revision. Repeated writes by the same candidate retain the baseline. Host writes retire the entry, so a later candidate write records the latest authoritative baseline. Rollback restores this metadata directly and advances frame revision without running correction policy or invalidating unrelated committed main turns. The journal does not undo tool side effects or revive cancelled tasks.

The compatibility extractor runs at most one request at a time and coalesces later changes into the latest pending input. Results must match the current candidate text and audio identity; request order remains recorded for accepted slot updates, including Clear. Per-slot host-write barriers prevent older extractions from overwriting host changes without blocking unrelated slots. A bounded overflow fails closed. Lexical cues are provisional and do not invalidate tasks; extraction compares against the authoritative baseline rather than treating a cue as a verified value. If an audio Final arrives while extraction for that candidate is relevant, the runtime retries against the assembled final text and waits at most the configured extraction timeout before committing. On failure, it restores the last accepted candidate value or its original baseline.

## Dependency-scoped invalidation

An accepted slot change, including setting a previously absent value or clearing an existing one, invalidates tasks and stored speculative or deep results that declare that slot as a dependency. An unchanged accepted value does not cancel work. The thought epoch increases, but unaffected active tasks retain their original task identity and epoch rather than being silently made stale. A cancelled main turn is resolved once; cancelling it does not undo an external effect. Provisional lexical cues are not sufficient on their own to cancel a task.

## Speculative read / Deep worker

- **Speculative read**: When intent confidence for a stable prefix exceeds the threshold, run a preliminary request in a dedicated YAML/RuntimeAgent instance that registers only pure-read tools (a separate instance is required because root turns are serialized). Store the result with a dependency fingerprint and include it in the main turn's context brief under `speculative_results:`.
- **Deep worker**: A separate runtime for long-running work. Admission requires an outstanding deep task ID and its original epoch; the session consumes that ownership once and uses the dependencies recorded at request time. An unrelated slot change does not invalidate it; a relevant change, timeout, or new main turn does. Accepted results are included in the main turn's context brief under `deep_worker_results:`. An externally emitted `DeepWorkResult` must also match an outstanding task and cannot inject an unregistered or duplicate result.

  **The deep worker has no automatic trigger.** The agent stream has no signal that “this turn will take a long time,” so the host explicitly requests it through `VoiceSessionHandle::request_deep_work`. The session is responsible only for epoch scoping and result injection. The scenario DSL can drive this path with a `deep_work_requested` event.

Both features are disabled by default in config, and their behavior is regression-tested with scenarios.

## Session isolation

`RuntimeAgent` holds conversation memory and serializes root turns per runtime. Therefore, **create a new brain for each session** (`BrainProvider`). Sharing one would mix transcripts from different clients and cause concurrent sessions to block each other.

Only the tool backend (`ReservationStore`) is shared, as it models an external system. Text injection queues are also isolated per session.

## Failure recovery

| Situation | Behavior |
|---|---|
| ASR connection closes or errors | Emit `ProviderError`, reset the stream generation so work derived from abandoned partials is invalidated, then reconnect fresh with backoff from 250ms to 8s. Audio between connections is discarded; it is never replayed as recognized speech |
| ASR open rejected or a non-retryable in-session error | Report and stop the ASR source; it is not retried until the next session start. Typed text injection keeps flowing |
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
