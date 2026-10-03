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

-- full-duplex-demo (transport layer) ----------------------------------------
Layer 3b gateway E2E         in-process axum + real WebSocket
                             (full-duplex-demo/tests/gateway_e2e.rs)
         demo config         validates the demo agent YAML schema
                             (full-duplex-demo/tests/demo_config.rs)
         agent specs         runs demo agent specs with the real runtime + mock LLM;
                             checks that the deep worker spec runs without voice
                             context and the task agent spec rejects missing context
                             (full-duplex-demo/tests/agent_specs.rs)
         browser script      uses the real inline script + mock WebSocket/AudioContext
                             to check binary PCM reception and playback ACKs
                             (Node's built-in test runner)
                             (full-duplex-demo/tests/web_playback.test.cjs)

-- manual --------------------------------------------------------------------
Layer 5  acoustic replay     `WavFrameSource` + `replay_wav_realtime`, real microphone
```

Layer 3 is split because the library has no transport layer. `runtime_pipeline` checks only the channel interface exposed by `VoiceRuntime`; the demo's gateway checks the actual axum integration.

## Running tests

```bash
# Library only (default; the fastest path for routine development)
cargo test

# Entire workspace, including the demo
cargo test --workspace

# Deterministic core only (without the framework)
cargo test --no-default-features

# Real WebSocket gateway E2E (in the demo)
cargo test -p full-duplex-demo --test gateway_e2e

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

## Duplex provider regressions

`cargo test --test duplex_providers` drives public provider traits and runtime injection with virtual-time test-local fakes. It covers capability dispatch, strict injected task-spec probing, provider/session isolation, actual PCM admission, fatal/transient open behavior, input saturation, typed injection during reconnect, native audio before finish, buffered finish/byte limits, standalone native convenience ownership, cache hits in the actual runtime, and Drop behavior. Output-unconsumed tests verify absolute native deadlines and buffered timeout delivery through a separate terminal slot even with a full audio queue.

Supervisor regressions directly assert metadata/absence-preserving journal rollback, host/candidate interleaving, matching closure and stale extraction rejection, failed-reply disposal, partial-audio Abort and ACK settlement, exactly-once resolutions, ownerless new-turn fast settlement, stale ACK/watchdog isolation, Never protection after synthesis completes, preserved deferred commit context, latest deferred supersession, watchdog-driven release, and owned worker cleanup. Provider normalization rejects zero/changeable rates, odd/oversized PCM, and invalid sequences while bounding output chunks.

Paid contracts are still ignored by default. They use actual outer timeouts across open and I/O, require an authoritative ASR Final, transmit fixture audio concurrently with transcript drain, and require valid TTS audio plus successful terminal and bounded cleanup. Official provider docs and local fakes are not evidence that a real account/model/locale combination works.

## Platform validation

**The supported targets are native Linux, Windows, and macOS runtimes.** Changing development hosts does not remove support for other operating systems or make WSL/Docker a substitute for native execution. Validate both the default `framework` feature and the `--no-default-features` core.

All four commands must pass in a native environment before basic Rust validation for that environment is complete. `.github/workflows/ci.yml` configures separate `ubuntu-latest`, `windows-latest`, and `macos-latest` jobs on `push` and `pull_request`.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test --no-default-features
```

You can additionally check core lints with `cargo clippy --no-default-features --all-targets -- -D warnings`. Run the browser script's Node test:

```sh
node --test full-duplex-demo/tests/web_playback.test.cjs
```

The test applies `node --check` to the actual inline script in `full-duplex-demo/web/index.html` and checks `startSession`'s `binaryType` and `onmessage` setup through mock WebSocket PCM reception and the ACK after mock playback completes. CI is configured to install Node 20 and run this command. It does not simulate a browser, AudioWorklet, physical playback, or microphone permissions, so perform the [manual browser playback check](deployment.md#manual-browser-playback-ack-check) separately.

### Evidence available so far

The automated checks below ran locally on Windows; a user additionally reported the Windows Edge browser check on 2026-09-29. A subsequent Windows GitHub Actions lint run using Rust 1.98 reported `chunks_exact_to_as_chunks` as an error under `-D warnings`. The PCM decoder now uses `as_chunks::<2>()` without suppressing the lint. Local workspace Clippy passed with Rust 1.93.1 and 1.97.0 after this change; Rust 1.98 and the complete three-OS CI matrix still require a rerun.

| Environment | Evidence available | Not yet verified |
|---|---|---|
| Linux | Basic checks were reported passing in a previous environment | Rerun on the current revision; compatibility by distribution and target |
| Windows `x86_64-pc-windows-msvc` | Basic checks passed with Rust 1.93.1: workspace 115 passed / 2 ignored; core 101 passed / 2 ignored. Script syntax and mock binary playback tests: 2 passed with Node 24. On 2026-09-29, a user reported Edge receiving `speech_started`, binary PCM and `speech_done`, sending both playback ACKs after text injection, with no Console errors; the browser reported 48 kHz capture and the server initialized 48 kHz → 16 kHz resampling | Rust 1.98 CI rerun and Node 20 results; microphone VAD/recognition, audible speech, other Windows targets and browsers |
| macOS | Supported target, but no native execution results | Build, lint, tests, Intel and Apple Silicon targets, browser/microphone E2E |

After the decoder lint fix, local Windows tests passed with 117 workspace tests and 103 framework-free tests (2 paid contract tests ignored in each run). Decoder tests cover explicit little-endian sample values, empty payloads, short headers, and odd-length payload rejection. These tests preserve the existing wire contract; they do not replace checking the reported lint on the CI toolchain.

Passing on one operating system or CPU architecture does not validate another target. Successful cross-compilation does not replace execution checks; a Linux Docker build does not validate native Windows or macOS.

### Manual validation record

- Record the server OS, CPU, Rust target/toolchain, client OS, and browser version separately.
- Check microphone permissions, actual capture sample rate, text injection, binary chunk reception and playback ACKs, duck/resume/abort, and reconnection. Mock TTS is silent, so verifying audible natural speech requires a separate test with a real TTS provider.
- Browser tests are separate from the server's Rust gateway E2E tests. Test supported browsers in person, including Safari on macOS if applicable.
- Together/Qwen contract tests, heard-memory behavior with a real LLM, long-running stability, and Docker builds are separate checks. An ignored provider contract test in the default suite is unverified, not a pass or a failure.

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
    heard_contains: "[중단됨"     # Optional
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
| `transactional_claim_requires_action_commit` | An execution record is required before speaking "완료되었습니다" |
| `transactional_action_commit_is_order_independent` | An Action Commit is produced regardless of ToolCompleted/ToolExecuted arrival order |
| `transactional_claim_blocked_without_execution_record` | The same claim is rejected without an execution record |
| `progress_speech_announces_a_slow_tool` | ToolStarted on a slow tool produces Process speech; the Factual final is played later |
| `tts_failure_does_not_wedge_the_speech_queue` | Discards the failed reply's remaining clauses, attempts the next independent reply, and resolves each reply exactly once as undelivered |
| `proactive_backchannel_fills_an_incomplete_pause` | An incomplete ending and silence between the soft and hard thresholds trigger a proactive "네" |
| `deep_work_result_reaches_the_session` | A host-initiated deep-work request applies its result when the epoch matches |
| `playback_ack_timeout_releases_the_speech_queue` | A client that sends no ACK does not occupy the speech queue forever |
| `hard_stop_overrides_an_uninterruptible_act` | An explicit "그만" overrides the act's `Interruptibility::Never` |
| `fully_heard_reply_is_resolved_unchanged` | A fully played reply stays unchanged in memory; the user message contains only the utterance, and turn environment goes in context |
| `interrupted_reply_keeps_only_the_heard_prefix` | An interrupted reply retains only the heard prefix and marker; canceled synthesis is not counted as TTS failure |
| `stale_reply_is_discarded_from_memory` | A stale, unplayed reply is rolled back as a whole turn |
| `unplayed_reply_is_recorded_as_undelivered` | When TTS fails before any speech plays, memory records `[응답이 전달되지 않음]` |
| `rejected_claim_is_recorded_as_undelivered` | A transactional claim rejected by the claim gate is not recorded in memory as delivered |
| `superseded_turn_is_discarded` | A new committed turn during inference cancels and rolls back the earlier turn |

## Notes

- Scenarios run under virtual time with `#[tokio::test(start_paused = true)]`. The session control tick, TTS chunk cadence, and simulated playback share the same virtual clock.
- `SimulatedPlaybackSink` "plays" chunks like a browser and returns `PlaybackProgress/Completed/Interrupted` ACKs through the session channel to check the end-to-end flow.
- Task cancellation expectations are captured before shutdown, which normally cancels all remaining tasks.
- **A bug-fix test should fail against the pre-fix code.** Assert the behavior that was broken. For example, the backchannel regression test checks `voice_backchannel_emitted_total`; mutually exclusive gate conditions kept that counter at zero in the previous implementation.
- When adding a scenario, the runner prints commands and events to stderr on failure so the cause is visible in the logs.
