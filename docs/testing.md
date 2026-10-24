# Testing

Testing has five layers. Layers 1–3 can run without paid APIs, and `.github/workflows/ci.yml` is configured to check them on native Linux, Windows, and macOS runners. Actual workflow results must be checked separately. Layers 4–5 must be run explicitly.

```text
-- voice-live (root package) -------------------------------------------------
Layer 1  unit tests          in src/, a few milliseconds
Layer 2  scenario suite      virtual-time, full-session simulation (tests/scenarios.rs)
Layer 3a runtime channel E2E channels only, without a transport layer (tests/runtime_pipeline.rs)
Layer 3c framework memory   real ai-agents runtime + MockLLMProvider
                             (tests/framework_memory.rs, framework feature)
Layer 4  provider contract   paid API required, #[ignore] (tests/provider_contracts.rs)

-- examples/browser (transport layer) ----------------------------------------
Layer 3b gateway E2E         in-process axum + real WebSocket
                             (examples/browser/tests/gateway_e2e.rs)
         example config      validates the example configs and agent YAML schema
                             (examples/browser/tests/demo_config.rs)
         agent specs         runs example agent specs with the real runtime + mock LLM;
                             checks that the deep worker spec runs without voice
                             context and the task agent spec rejects missing context
                             (examples/browser/tests/agent_specs.rs)
         browser script      uses the real inline script + mock WebSocket/AudioContext
                             to check binary PCM reception and playback ACKs
                             (Node's built-in test runner)
                             (examples/browser/tests/web_playback.test.cjs)

-- manual --------------------------------------------------------------------
Layer 5  acoustic replay     `WavFrameSource` + `replay_wav_realtime`, real microphone
```

Layer 3 is split because the library has no transport layer. `runtime_pipeline` checks only the channel interface exposed by `VoiceRuntime`; the browser example's gateway checks the actual axum integration.

## Running tests

```bash
# Library only (default; the fastest path for routine development)
cargo test

# Entire workspace, including the browser example
cargo test --workspace

# Deterministic core only (without the framework and the network providers)
cargo test --no-default-features

# Real WebSocket gateway E2E (in the browser example)
cargo test -p browser-example --test gateway_e2e

```

Paid provider checks require API keys and a consented Korean 16 kHz mono PCM16 WAV file, and may incur charges. Set the fixture path for the host you are using.

Bash, Zsh, or Git Bash:

```bash
export VOICE_KOREAN_FIXTURE="/absolute/path/korean-16k-mono.wav"
```

Windows PowerShell:

```powershell
$env:VOICE_KOREAN_FIXTURE = "C:/audio/korean-16k-mono.wav"
```

After setting the environment variable, run:

```sh
cargo test --test provider_contracts -- --ignored
```

## Language-neutral policy and strict speech regressions

`cargo test --test language_neutral` checks neutral analysis, explicit non-language-mode rules, long backchannels and hard-stop priority, invalid/empty-matching regex rejection, load errors for removed speech keys, syntax-valid non-English/Korean ASR wire selectors (including fake-only `zz`), and strict outcome/receipt/target checks without model-supplied call IDs. It asserts that generated reply text in any language reaches TTS with its original Final identity; that malformed replies, unknown or unconfirmed outcomes, and stale proofs are blocked before TTS; that a failed write in the turn replaces any reply, including a plain-text success claim, with the configured failure text while a failed `pure_read` lookup or a call to a tool that never ran does not; and that a neutral configuration without failure text stays silent. TTS wire tests check region/script selectors on continuation and finish. Fake-only selector acceptance is not vendor/model/voice compatibility evidence.

With the framework feature, `cargo test --test language_neutral_framework` drives the published SDK with a mock LLM and bound tool, verifies execution-context proof ownership, and checks that heard text replaces the exact original JSON Final in actual framework memory. With SDK tool events both off and on, a bound tool reports exactly one start and one completion carrying its output. It also runs a full session through the framework agent: a reply naming an outcome after the tool executed reaches TTS, and the same reply without the tool run is replaced by the failure text. Proof-module regressions cover changed execution arguments, repeated attempts under one origin, oversized identities before snapshot retention, released-origin late records, and effect-before-receipt cancellation without rollback or automatic retry. The framework integration also disables SDK tool-event streaming while asserting canonical start/completion events from the bound executor path. Runtime composition tests exercise all host overrides through real sessions and verify per-session extractor factory calls. Reservation receipt tests reject an idempotent result for a changed window-seat request. Supervisor regressions cover queued progress completion/expiry, queued progress retired when the task ends by reply or failure, the brief's `speech_language` line only when configured, strict authorization across recovery epochs, and question hints that require actual playback completion.

The existing Korean scenarios explicitly install Korean example rules to retain their historical coverage; replies run through the same reply path as everything else, and `ScenarioRunner` itself defaults to neutral rules. New scenarios supply explicit rules when they need them, and outcome rules when a reply names an outcome. No scenario DSL field or browser wire message was added. `CognitiveEvent`/`VoiceEvent` consumers must handle the additive `InvocationProof` event, which is intentionally not logged with its arguments/output.

Semantic supervisor tests exercise injected and final-only inputs, single-flight coalescing, stale decisions, actual-heard context, interruption, neutral timeout (counted in `voice_semantic_timeout_total`), and speculative invalidation; a framework unit test checks that a failing semantic model is reported and that recovery clears it. VAD unit tests check that muted input does not turn room noise into speech, that a rise in steady noise does not hold speech on, and that speech with short gaps stays on past the floor recheck. `language_neutral_framework` uses a mock LLM provider to verify current-only model context and per-session runtime construction without a real key. These checks do not prove real model interpretation or device behavior.

These checks are offline and do not rerun or supersede historical paid English results. Physical microphone/browser behavior, real model adherence to the reply format and `speech_language`, supported language combinations, and naturalness still require separate validation.

## Duplex provider regressions

`cargo test --test duplex_providers` drives public provider traits and runtime injection with virtual-time test-local fakes. It covers capability dispatch, strict injected task-spec probing, provider/session isolation, actual PCM admission, fatal/transient open behavior, input saturation, typed injection during reconnect, native audio before finish, buffered finish/byte limits, standalone native convenience ownership, cache hits in the actual runtime, and Drop behavior. Output-unconsumed tests verify absolute native deadlines and buffered timeout delivery through a separate terminal slot even with a full audio queue.

Supervisor regressions cover active-VAD commit rejection, candidate and host interleaving, same-revision extraction ordering, revised and final input, timeout recovery, Clear and absence-to-value invalidation, unaffected main/deep results, late deep events, and exactly-once turn resolution after worker termination. They also cover journal rollback, playback recovery, question audibility, Never protection, and worker cleanup. The four former pending slot-ordering/invalidation regressions now run in the normal library suite. The framework slot parser has focused malformed, oversized, mixed, and valid-response tests. Provider normalization rejects zero/changeable rates, odd/oversized PCM, and invalid sequences while bounding output chunks.

Paid contracts are still ignored by default. They use actual outer timeouts across open and I/O, require an authoritative ASR Final, transmit fixture audio concurrently with transcript drain, and require valid TTS audio plus successful terminal and bounded cleanup. Official provider docs and local fakes are not evidence that a real account/model/locale combination works.

## Cartesia provider regressions and paid contracts

`cargo test --test cartesia_providers` runs real localhost WebSocket fakes, not Cartesia endpoints. It checks header/version authentication, unchanged context settings, verbatim fragments, audio before finish, normal and audio-bearing terminals, context-less errors before/after input, wrong contexts, base64/PCM/format budgets, queue saturation with reserved finish, independent absolute deadlines, context expiry, cancellation/Drop, cancelled output polls, legacy ownership, and factory-only validation. The target also verifies native premade warmup/cache reuse and a channel runtime path from actual PCM/VAD through both Cartesia adapters to authorized playback, chunk/completion ACKs, and exactly-once turn resolution, without text injection or paid providers. It also counts the silent samples a fake recognizer receives before each finalize, so a manual commit sent before `asr.commit_delay_ms` of trailing audio, a short pause split into two utterances, or a missing idle commit after `asr.idle_commit_ms` of silence fails. Socket tests use bounded real time; runtime policy regressions use paused virtual time.

`cargo test --test cartesia_asr` covers manual STT: header/query construction, raw little-endian PCM and plain-text finalize/close, exact delta concatenation and tail replacement, connection identity versus utterance identity, monotonic revisions, duplicate snapshot suppression, bounded PCM/command permits including writer-held data and outstanding flushes, pipelined finalize, continuous-silence/repeated-flush stress with positive ack latency, pending/finish deadlines, graceful confirmed-only drain, full/unpolled output, Ping handling during pacing, EOF and malformed events, redacted retry classification, cancelled admission/output polls, legacy facade, factory/injection validation, and Drop ownership. Regressions preserve acknowledged Finals across later receive failure and preserve the original finish deadline after finished-input Drop. Another checks that 20 ms frames reach the wire as `chunk_ms` messages and that held audio is sent before a finalize.

`tests/duplex_providers.rs` additionally verifies non-retryable ASR errors racing with input closure and preservation of TTS provider errors before text admission. Supervisor regressions cover consumed audio after partial commit, preserving a successor hypothesis and transcript epoch, Final-first empty successors, mixed injection/audio, same-text fresh identities, and reset behavior. The scenario `partial_commit_does_not_repeat_on_late_audio_final` asserts exactly one committed turn instead of two. These regressions were first observed failing against the old behavior before their fixes.

The ignored test `cartesia_tts_korean_streaming_contract` requires `CARTESIA_API_KEY` and an approved `CARTESIA_VOICE_ID`. It sends two synthetic Korean text fragments, requires valid first audio before finish, drains a normal terminal, and bounds open/request/cleanup. It does not use the LLM or `VOICE_KOREAN_FIXTURE`. Run only after account, cost, and external-transmission approval:

```sh
cargo test --locked --test provider_contracts cartesia_tts_korean_streaming_contract -- --ignored --nocapture
```

The ignored `cartesia_stt_korean_streaming_contract` requires `CARTESIA_API_KEY` and `VOICE_KOREAN_FIXTURE`, a consented mono 16 kHz signed PCM16 Korean WAV of at most 15 seconds. It transmits the fixture twice at real-time pace, continues PCM while flushes are pending, and requires two nonempty ordered utterance Finals, normal done/closure, and a nonempty partial before the first finalize admission. It reports first-partial/total elapsed time on the host's monotonic clock, not mixed client/server clocks. A failure of the pre-finalize partial requirement means the selected model/fixture has not established streaming UX; it must be investigated, not silently waived or replaced with another model.

```sh
cargo test --locked --test provider_contracts cartesia_stt_korean_streaming_contract -- --ignored --nocapture
```

The two Korean contracts compile in the default suite but remain ignored and have not been run against Cartesia. The adapter's FIFO association of finalizes and `flush_done` is an explicit integration assumption; fake-server success and even a finite live fixture run do not establish all vendor silence/overlap semantics. Inspect repeated utterances, silence auto-finalization, empty flushes, first-partial cadence/latency, and boundary attribution during subsequent live validation. PCM validity is not evidence that natural speech sounds correct; listening, real microphone input, physical interruption, actual LLM heard-memory, and browser playback need separate checks.

### English provider-only validation

The English contracts generate their own synthetic fixture, so no microphone recording, `VOICE_KOREAN_FIXTURE`, or LLM key is needed. Run them in order from a shell containing `CARTESIA_API_KEY` and `CARTESIA_VOICE_ID`. Keys assigned in PowerShell `$PROFILE` are available to its child processes; loading the profile is an explicit shell step, not runtime behavior. Do not print the profile or credentials.

```sh
cargo test --locked --test provider_contracts cartesia_tts_english_streaming_contract -- --ignored --exact --nocapture
cargo test --locked --test provider_contracts cartesia_stt_english_streaming_contract -- --ignored --exact --nocapture
```

The first test sends two English text fragments, requires audio before finish and a normal terminal, and writes a mono 16 kHz signed PCM16 WAV and metrics into a git-ignored local artifact directory. The second test reuses that generated WAV twice, checks ordered nonempty Finals and expected content, requires a partial before the first finalize admission, and records metrics before asserting the acceptance criteria. Both remain ignored in the default suite. Audio, local logs, account voice IDs, and credentials must not be committed.

On 2026-10-18 UTC, the English tests passed against Cartesia after explicitly enabling Rustls's `ring` backend. The initial attempt failed at client-side crypto-provider selection before synthesis; it was not a provider streaming failure. The Cargo change selects the missing backend without disabling TLS certificate verification, and a local regression exercises crypto-provider selection.

| Check | Observed result |
|---|---|
| Requested configuration | `sonic-3.6` TTS, `ink-whisper` STT, API version `2026-08-14`, language `en` |
| TTS first audio before finish | 185 ms after text admission on the open socket |
| Generated fixture | 4.88 seconds, mono 16 kHz PCM16, 30 normalized chunks |
| STT first replacement Partial | 2568 ms after feeding began on the open socket |
| STT updates and boundaries | 4 Partials, 2 nonempty Finals in order, normal completion |
| Recognition content | Both repetitions matched the expected streaming/Monday/ten-or-10 content |

Synthetic input was: `This is a streaming speech test. Please check the appointment for Monday at ten.` Both recognized Finals were: `This is a streaming speech test. Please check the appointment for Monday at 10.` Leading provider whitespace is omitted here for display. These are individual observations, not latency percentiles; they exclude socket establishment from the first-audio/first-partial measurements. Actual billed amounts were not queried.

This verifies the limited English provider flow, not the Korean contracts, physical microphone or speaker behavior, naturalness, browser E2E, English agent prompts/turn-taking, or every silence/overlap/FIFO case. English provider normalization does not change the Korean demo defaults or establish multilingual runtime validation.

## Platform validation

**The supported targets are native Linux, Windows, and macOS runtimes.** Changing development hosts does not remove support for other operating systems or make WSL/Docker a substitute for native execution. Validate both the default `framework` feature and the `--no-default-features` core.

All four commands must pass in a native environment before basic Rust validation for that environment is complete. `.github/workflows/ci.yml` configures separate `ubuntu-latest`, `windows-latest`, and `macos-latest` jobs on `push` and `pull_request`.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test --no-default-features
```

Tests that need a network speech provider are gated on its feature: `tests/cartesia_asr.rs` and `tests/cartesia_providers.rs` on `cartesia`, `tests/provider_contracts.rs` on `cartesia`, `together`, and `qwen` together. Without `cartesia`, `tests/runtime_pipeline.rs` checks that selecting the provider fails with an error naming the missing feature.

You can additionally check core lints with `cargo clippy --no-default-features --all-targets -- -D warnings`. Run the browser script's Node test:

```sh
node --test examples/browser/tests/web_playback.test.cjs
```

The test applies `node --check` to the actual inline script in `examples/browser/web/index.html` and checks `startSession`'s `binaryType` and `onmessage` setup through mock WebSocket PCM reception and the ACK after mock playback completes. A second test denies the microphone and checks that the page closes the socket, shows why, and re-enables Start. CI is configured to install Node 20 and run this command. It does not simulate a browser, AudioWorklet, physical playback, or microphone permissions, so perform the [manual browser playback check](deployment.md#manual-browser-playback-ack-check) separately.

### Evidence available so far

The automated checks below ran locally on Windows; a user additionally reported the Windows Edge browser check on 2026-09-29. A subsequent Windows GitHub Actions lint run using Rust 1.98 reported `chunks_exact_to_as_chunks` as an error under `-D warnings`. The PCM decoder now uses `as_chunks::<2>()` without suppressing the lint. Local workspace Clippy passed with Rust 1.93.1 and 1.97.0 after this change; Rust 1.98 and the complete three-OS CI matrix still require a rerun.

| Environment | Evidence available | Not yet verified |
|---|---|---|
| Linux | Basic checks were reported passing in a previous environment | Rerun on the current revision; compatibility by distribution and target |
| Windows `x86_64-pc-windows-msvc` | Basic checks passed with Rust 1.93.1: workspace 115 passed / 2 ignored; core 101 passed / 2 ignored. Script syntax and mock binary playback tests: 2 passed with Node 24. On 2026-09-29, a user reported Edge receiving `speech_started`, binary PCM and `speech_done`, sending both playback ACKs after text injection, with no Console errors; the browser reported 48 kHz capture and the server initialized 48 kHz → 16 kHz resampling | Rust 1.98 CI rerun and Node 20 results; microphone VAD/recognition, audible speech, other Windows targets and browsers |
| macOS | Supported target, but no native execution results | Build, lint, tests, Intel and Apple Silicon targets, browser/microphone E2E |

After the decoder lint fix, local Windows tests passed with 117 workspace tests and 103 framework-free tests (2 paid contract tests ignored in each run). Decoder tests cover explicit little-endian sample values, empty payloads, short headers, and odd-length payload rejection. These tests preserve the existing wire contract; they do not replace checking the reported lint on the CI toolchain.

Passing on one operating system or CPU architecture does not validate another target. Successful cross-compilation does not replace execution checks; a Linux container build does not validate native Windows or macOS.

### Manual validation record

- Record the server OS, CPU, Rust target/toolchain, client OS, and browser version separately.
- Check microphone permissions, actual capture sample rate, text injection, binary chunk reception and playback ACKs, duck/resume/abort, and reconnection. Mock TTS is silent, so verifying audible natural speech requires a separate test with a real TTS provider.
- Browser tests are separate from the server's Rust gateway E2E tests. Test supported browsers in person, including Safari on macOS if applicable.
- Together/Qwen contract tests, heard-memory behavior with a real LLM, and long-running stability are separate checks. An ignored provider contract test in the default suite is unverified, not a pass or a failure.

## Scenario DSL

Each YAML file is a regression test. The runner creates a session, injects a timeline under virtual time, and evaluates the expectations.

The following is a **field catalogue, not a runnable scenario**: its `expect` list shows alternative checks from different tests, including mutually exclusive commit expectations. Choose only the expectations appropriate for your case; see `tests/scenarios.rs` for runnable examples.

```yaml
name: backchannel_during_agent_speech

# State before the session starts
initial:
  assistant_speaking: true          # Start during playback
  assistant_asked_question: false
  assistant_recent_text: "..."      # Recent utterance seed for echo detection
  semantic_frame:
    date: "금요일"                   # Initial slot value
  active_tasks:
    - id: availability_search
      dependencies: [date, time, party_size]

# Event timeline in virtual milliseconds
timeline:
  - at_ms: 0
    event: { type: local_speech_started, vad_probability: 0.9 }
  - at_ms: 80
    event: { type: asr_partial, revision: 1, text: "네" }
  - at_ms: 180
    event: { type: local_speech_ended }

# FakeAgent script (times relative to the start of the turn)
agent_script:
  - after_ms: 100
    type: tool_started
    tool: search_availability
  - after_ms: 200
    type: tool_executed
    tool: search_availability
    executed: true
  - after_ms: 300
    type: tool_completed
    tool: search_availability
    success: true
  - after_ms: 400
    type: final
    text: "예약이 완료되었습니다."

# Optional config overlay (merged with defaults)
config:
  tools:
    effects:
      reserve_restaurant: transactional_write

expect:
  - { type: command, command: duck }
  - { type: command_within, command: duck, within_ms: 120 }
  - { type: never_command, command: abort }
  - { type: event, event: playback_completed }
  - { type: never_event, event: user_turn_committed }
  - { type: user_turn_committed, contains: "예약해줘" }
  - { type: no_user_turn_committed }
  - { type: committed_turn_count, exactly: 1 }
  - { type: task_cancelled, id: availability_search }
  - { type: task_not_cancelled, id: user_profile_lookup }
  - { type: thought_epoch_incremented }
  - { type: audible_contains, text: "멈췄어요" }
  - { type: audible_excludes, text: "예약이 완료되었습니다" }
  - { type: metric_at_least, name: voice_claim_rejected_total, value: 1 }

settle_ms: 2500        # Additional time after the timeline ends
agent_final: "..."     # Default reply when agent_script is absent

# Optional environment controls
tts_chunks_per_request: 3     # 0 makes synthesis fail (tests TTS failure recovery)
playback_acks: true           # false simulates a client that sends no playback ACKs
deep_work_summary: "..."      # Installs FakeDeepWorker when set
deep_work_delay_ms: 200
```

### Turn-memory expectations

Scenarios cannot inspect framework memory directly, so they check requests and resolutions recorded by `FakeAgent`. The resolution's `heard` text is what replaces the generated reply in memory. The fragment below is another catalogue of optional checks; select compatible conditions for a real scenario.

```yaml
expect:
  - type: turn_resolved          # Passes if any one resolution meets every condition
    resolution: audible          # audible | discard
    heard_equals: "..."          # Optional
    heard_contains: "[voice-live:interrupted]"     # Optional
    heard_excludes: "가능합니다"   # Optional
    heard_equals_final: false    # Optional; true means fully heard
  - type: turn_resolution_count
    exactly: 1
  - type: agent_input_equals     # User message for every main turn
    text: "토요일 저녁 7시로 예약해줘"
  - type: agent_context_contains # Turn environment for every main turn
    text: "idempotency_key: voice-turn-"
```

`tests/framework_memory.rs` uses `MockLLMProvider::call_history()` to verify what the model receives and what is stored in actual framework memory.

### Timeline event types

`local_speech_started`, `local_speech_ended`, `asr_partial`, `asr_final`, `asr_stream_reset`, `interaction_decision`, `tool_started`, `tool_completed`, `tool_executed`, `agent_final`, `deep_work_requested`, `deep_result`.

The `deep_result` timeline event creates an uncorrelated task ID and is therefore observed in the trace but not admitted as an actual deep result. Use `deep_work_requested` with a configured fake deep worker when testing result admission.

`asr_partial` and `asr_final` accept an optional `utterance_id` for identity tests. With automatic IDs, a matching Final or a trailing partial after VAD end retains the recognizer identity; VAD end alone does not increment it. The next speech start or the next recognizer event after a Final advances the automatic ID. The DSL defaults to the audio origin. `asr_stream_reset` carries `generation` and drives uncommitted candidate invalidation without removing authoritative finals or committed turns.

`metric_at_most` checks a counter's upper bound. Combine it with `metric_at_least` to require an exact value; `turn_resolution_count` checks exactly-once resolution directly rather than inferring it from a failure counter.

## Current scenario suite

| Scenario | What it checks |
|---|---|
| `backchannel_ducks_then_resumes` | "네" during an explanation → duck within 120 ms, resume, no abort or commit |
| `correction_invalidates_date_dependent_search` | Changing the date cancels only dependent tasks, keeps the profile task, and increments the epoch |
| `hard_interruption_aborts_and_acks` | "잠깐" → immediate abort and actual playback of the "네, 멈췄어요." ACK |
| `answer_after_agent_question_commits` | "네" after a question commits as an answer, not a backchannel |
| `incomplete_ending_waits_and_merges` | Waits after an incomplete ending such as "-는데/..." and merges the continuation into one turn |
| `self_echo_is_not_a_user_turn` | Assistant speech returned through STT causes neither a commit nor an abort |
| `full_pipeline_commits_and_speaks` | Full commit → agent → claim gate → TTS → playback path |
| `stale_final_is_never_spoken` | A final result from an earlier epoch is discarded after a correction and never played |
| `executed_write_records_an_action_commit` | An executed, successful write produces an Action Commit (`voice_action_commit_total`) |
| `transactional_action_commit_is_order_independent` | An Action Commit is produced regardless of ToolCompleted/ToolExecuted arrival order |
| `transactional_claim_blocked_without_execution_record` | A reply naming the `reserved` outcome is not spoken when no execution record backs it |
| `progress_speech_announces_a_slow_tool` | ToolStarted on a slow tool produces Process speech; the Factual final is played later |
| `tts_failure_does_not_wedge_the_speech_queue` | Discards the failed reply's remaining clauses, attempts the next independent reply, and resolves each reply exactly once as undelivered |
| `proactive_backchannel_fills_an_incomplete_pause` | An incomplete ending and silence between the soft and hard thresholds trigger a proactive "네" |
| `deep_work_result_reaches_the_session` | A host-initiated deep-work request applies its result when the epoch matches |
| `playback_ack_timeout_releases_the_speech_queue` | A client that sends no ACK does not occupy the speech queue forever |
| `hard_stop_overrides_an_uninterruptible_act` | An explicit "그만" overrides the act's `Interruptibility::Never` |
| `fully_heard_reply_is_resolved_unchanged` | A fully played reply stays unchanged in memory; the user message contains only the utterance, and turn environment goes in context |
| `reply_longer_than_the_audible_history_resolves_as_heard` | A reply with more clauses than `speech.audible_history_max_clauses` that played in full is recorded unchanged, not as undelivered |
| `interrupted_reply_keeps_only_the_heard_prefix` | An interrupted reply retains only the heard prefix and marker; canceled synthesis is not counted as TTS failure |
| `stale_reply_is_discarded_from_memory` | A stale, unplayed reply is rolled back as a whole turn |
| `unplayed_reply_is_recorded_as_undelivered` | When TTS fails before any speech plays, memory records `[voice-live:undelivered]` |
| `rejected_claim_is_recorded_as_undelivered` | A rejected outcome reply is recorded in memory as `[voice-live:undelivered]`, not as delivered |
| `superseded_turn_is_discarded` | A new committed turn during inference cancels and rolls back the earlier turn |

## Notes

- Scenarios run under virtual time with `#[tokio::test(start_paused = true)]`. The session control tick, TTS chunk cadence, and simulated playback share the same virtual clock.
- `SimulatedPlaybackSink` "plays" chunks like a browser and returns `PlaybackProgress/Completed/Interrupted` ACKs through the session channel to check the end-to-end flow.
- Task cancellation expectations are captured before shutdown, which normally cancels all remaining tasks.
- **A bug-fix test should fail against the pre-fix code.** Assert the behavior that was broken. For example, the backchannel regression test checks `voice_backchannel_emitted_total`; mutually exclusive gate conditions kept that counter at zero in the previous implementation.
- When adding a scenario, the runner prints commands and events to stderr on failure so the cause is visible in the logs.
