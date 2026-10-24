# Running and deployment

## Supported platforms and build prerequisites

The `voice-live` library and the browser example server target **native execution on Linux, Windows, and macOS**. They should not depend on a particular developer's OS, shell, or absolute paths. This applies both to the default build with the `framework` feature and to the core build without it. The library is a server-side Rust runtime; the browser is the demo client. This support scope does not imply that the Rust library supports WASM.

- All platforms: install stable Rust with rustup and the native build tools for your target. `cargo` and `rustc` must be available on your shell's PATH. The runtime explicitly enables Rustls's `ring` crypto backend for WSS; certificate verification and WebPKI trust roots remain enabled.
- Linux: install your distribution's build tools, including a C/C++ compiler and linker.
- Windows: for the MSVC target, install the Visual Studio Build Tools C++ components and Windows SDK. Windows ARM64 additionally needs a Clang compiler on PATH for `ring`. You can run directly from PowerShell; Git Bash, WSL, and Docker are not required. Shell-specific PATH setup belongs to the development environment, not hardcoded library configuration.
- macOS: install native build tools such as Xcode Command Line Tools.
- Node.js is used to validate the browser script; it is not required to run the Rust server.

Distinguish supported targets from actual validation results. Check OS, CPU architecture, Rust target, and browser separately; see [platform validation](testing.md#platform-validation) for current evidence and procedures.

## Running locally (no API keys required)

Run this command from the repository root; it is the same in Bash, Zsh, and PowerShell.

```bash
cargo run -p browser-example -- --config examples/browser/configs/en/voice-runtime.yaml
# Open http://localhost:8080 → Start session
```

`configs/en/` and `configs/ko/` are complete language sets: each holds the keyless `voice-runtime.yaml`, `voice-runtime.cartesia.yaml`, and the `restaurants.yaml` the demo reservation backend loads (`--restaurants <path>` overrides it). Both use the same language-neutral agent specs under `examples/browser/agents/`. For a library-only check without a browser, run `cargo run --example quickstart`.

Paths are resolved as follows:

- Relative paths passed to `--config` are relative to the **process working directory**. From another directory, supply the correct relative or absolute path.
- Relative `agents.*.spec` paths loaded with `VoiceRuntimeConfig::from_file` are relative to the **config file's directory**. `from_yaml_str` does not adjust these paths.
- The browser example serves its own `examples/browser/web/` directory. A relative path set via `VOICE_WEB_DIR` is relative to the working directory.
- Relative `observability.event_log_dir` paths are also relative to the working directory, independent of Cargo's build output location.

Use portable relative paths such as `../../agents/reservation.yaml` in shared YAML. `/` separators also work on Windows. Configure OS-specific absolute paths on each host instead of fixing them in the repository. Quote CLI paths containing spaces and match filename case exactly for case-sensitive filesystems such as those commonly used on Linux.

The keyless configs use `asr.provider: inject`, `dev.allow_text_injection: true`, and `agents.interaction.enabled: true`. Each supplies automatic resources in its language and a keyless mock reply (`speech.mock_reply`), but no text-rule fallback. If the semantic provider is unavailable, the session uses neutral timing and control rather than guessing language-specific meaning. The mock reply asks for reservation details rather than asserting invented availability. With `OPENAI_API_KEY` set, the task agent writes its own replies in the configured `tts.language`.

- Text typed in the input box at the bottom of the page becomes the user's utterance. It follows the same commit → agent → claim gate → TTS → playback path as speech.
- With the microphone enabled, you can also check full-duplex behavior such as duck/resume/abort using mock TTS.
- `allow_text_injection` works with **all** ASR providers, so you can use `provider: together` for real recognition while reproducing scenarios with text.

### Browser requirements and current limitations

The browser must support WebSocket, Web Audio, AudioWorklet, and `getUserMedia`. Microphone access requires browser and OS permissions and a secure context. For local development, use `http://localhost:8080`; for access from another device, use HTTPS/WSS. Currently, `Start session` requests microphone permission even when using only text injection. If permission is denied, the page closes the session socket, shows `microphone blocked` with a note in the log, and re-enables Start; allow the microphone for the site and start again. The sample `mock` TTS emits silent PCM, so it cannot verify that natural speech is audible.

The web client sets `ws.binaryType = 'arraybuffer'` and decodes the `ArrayBuffer` directly. `node --test examples/browser/tests/web_playback.test.cjs` checks the real inline script's WebSocket initialization, binary PCM delivery, and ACK after simulated playback completes. It covers more than `node --check` (syntax only), but does not guarantee actual audio output or microphone permission behavior in a browser/OS combination.

<a id="manual-browser-playback-ack-check"></a>

### Manual browser playback ACK check without API keys

1. From the repository root, run the `cargo run -p browser-example -- --config ...` command above. Without real keys configured, the sample uses inject ASR, mock TTS, a mock task agent, and neutral semantic fallback. If `OPENAI_API_KEY` is set, the task agent or the independently configured semantic provider can call a real LLM and transmit transcript/context text. Use a separate shell without the key for a key-free check.
2. On the same computer, open `http://localhost:8080` in a browser, then open developer tools to **Console** and **Network → WS → `/api/voice/ws` → Messages/Frames**.
3. Click **Start session** and allow microphone access. The example opens the microphone even when using only text injection. A remote browser not on `localhost` requires HTTPS/WSS.
4. Type `예약하고 싶어요` into the text input at the bottom and click **Send**. Check that received frames include `speech_started`, binary PCM chunks, and `speech_done` in that order. After playback ends, check sent frames for `playback_chunk_completed` and `playback_completed`. The Console should have no `TypeError`. Mock TTS is silent, so the chunks play without audible speech.
5. If this fails, check Console errors, WS frames, `provider_error` in the server logs, and `target/voice-traces/<date>/<session>/events.jsonl`. Stop the test with Ctrl+C in the server terminal.

This is a manual verification procedure, not a report of a completed run. Check real microphone input, device-specific sample rates, output devices, and barge-in separately for each OS/browser combination. Running the server on one OS does not validate browsers on other operating systems.

## Real providers

Linux/macOS Bash or Zsh, or Windows Git Bash:

```bash
export TOGETHER_API_KEY=...     # Nemotron streaming ASR
export DASHSCOPE_API_KEY=...     # Qwen realtime TTS
export QWEN_VOICE_ID=...        # TTS voice (variable referenced by tts.voice_env)
export OPENAI_API_KEY=...       # Task/Interaction Agent LLM
```

Windows PowerShell:

```powershell
$env:TOGETHER_API_KEY = "..."
$env:DASHSCOPE_API_KEY = "..."
$env:QWEN_VOICE_ID = "..."
$env:OPENAI_API_KEY = "..."
```

In `examples/browser/configs/<language>/voice-runtime.yaml`:

```yaml
asr:
  provider: together
tts:
  provider: qwen
agents:
  interaction:
    enabled: true    # Semantic interpretation uses the configured model
  task:
    spec: ../agents/reservation.yaml
```

First verify the provider adapters' wire fields with the paid contract tests:

```bash
cargo test --test provider_contracts -- --ignored
```

For an unverified provider combination, start diagnosis with the `provider_error` events in the logs and `target/voice-traces/<date>/<session>/events.jsonl`.

### When the example sends data out and can incur charges

Each of these happens on its own; changing one setting can start external calls even if everything else is still mock.

- **Semantic interpretation:** with `agents.interaction.enabled: true` (the default sample) and `OPENAI_API_KEY` present, each candidate utterance and the recently heard assistant text go to the model, even when the task agent is mock.
- **Task agent:** with `agents.task.spec` set and `OPENAI_API_KEY` present, every committed turn goes to the model.
- **Speculative reads and deep work:** off in the sample. Enabling `agents.speculative` or `agents.deep` adds model calls of their own.
- **Premade warmup:** the default sample has `tts.premade.enabled: true`. With mock TTS this is free, but switching `tts.provider` to `qwen` or `cartesia` synthesizes every premade phrase at startup, before any browser connects.
- **Real ASR/TTS:** a network ASR streams microphone audio from session start, and a network TTS is called for every spoken act.

A built-in network provider also needs an explicit `asr.locale` or `tts.language`; startup fails without one.

## Cartesia streaming STT and TTS

`examples/browser/configs/<language>/voice-runtime.cartesia.yaml` selects Cartesia Ink Whisper manual streaming STT and native Sonic TTS. The default configuration stays keyless. For typed-input TTS-only diagnosis, use `voice-runtime.cartesia-tts.yaml` instead; it deliberately keeps inject ASR.

Set `CARTESIA_API_KEY`, `CARTESIA_VOICE_ID`, and `OPENAI_API_KEY` in your server environment, then run:

```sh
cargo run -p browser-example -- --config examples/browser/configs/en/voice-runtime.cartesia.yaml
```

For Bash/Zsh/Git Bash, use `export CARTESIA_API_KEY=...` and `export CARTESIA_VOICE_ID=...`; for PowerShell, use `$env:CARTESIA_API_KEY = "..."` and `$env:CARTESIA_VOICE_ID = "..."`. Keep secrets out of committed files and browser code. The existing reservation task agent requires the OpenAI key; a failed task-spec probe is not silently replaced by a mock agent in this live-provider setup. The full configuration captures microphone audio; the TTS-only configuration still requests browser microphone permission but uses typed input for recognition. The server does not automatically load a `.env` file: export the variables into its process environment. If keys are assigned by PowerShell `$PROFILE`, launch the command from that profile-loaded PowerShell session; a previously launched editor shell does not inherit later profile assignments. A separate `.env` file is not required.

The Cartesia configs explicitly select `ink-whisper`, 16 kHz mono `pcm_s16le`, locale `ko-KR` or `en-US` (sent as the primary code `ko` or `en`), and `manual_commit: true`; local VAD enqueues finalizes without pausing PCM for acknowledgements. TTS selects `sonic-3.6`, raw mono PCM16 at 24 kHz, `ko` or `en`, and zero managed text buffering delay. Both use API version `2026-08-14`. Premade warmup is disabled; enabling it can incur synthesis costs at startup. STT traffic may begin at browser session start, before a complete utterance. Microphone audio and conversation text leave the host and can incur Cartesia/OpenAI charges. Approve account/voice use, cost limits, fixture consent, and data transmission before running; zero-data-retention is not assumed for a standard account.

Run the individual [ignored STT and TTS contracts](testing.md#cartesia-provider-regressions-and-paid-contracts) only after approval, then validate both providers in the browser. STT requires a consented Korean WAV through `VOICE_KOREAN_FIXTURE`; TTS requires an approved voice. The tests do not call the LLM. Socket upgrade is send-readiness, not proof of model/voice compatibility. Offline tests cover both adapters in one runtime. English TTS and STT have also passed the limited synthetic-input checks recorded in [testing](testing.md#english-provider-only-validation). Korean provider checks, browser/microphone behavior, naturalness, and the broader silence/overlap/cancellation cases remain unverified. In particular, FIFO transcript/flush attribution is an explicit assumption to verify, not an established vendor guarantee. See [STT boundaries and limits](configuration.md#cartesia-stt) and [TTS continuations](configuration.md#cartesia-tts).

## Host-supplied speech providers

A host can inject `Arc<dyn StreamingAsr>` and `Arc<dyn StreamingTts>` through `SpeechProviders` and `VoiceRuntime::build_with_providers`. See [injected settings and migration](configuration.md#injected-provider-settings-and-migration) and the [module contracts](modules.md#asr-tts). Factories are shared but each session/response has its own stream and control owner. No web framework, provider registry, or dynamic plugin loader is added to the library.

Legacy serial ASR implementations still compile for standalone `open`, but runtime injection requires a real independent duplex boundary. Legacy whole-text TTS factories are bridged once as `Buffered`; a native provider reports `Incremental` and implements append/finish with bounded input, owner-enforced absolute deadlines, cancellation-safe output, and Drop cleanup. `synthesize_via_text_stream` preserves a native provider's control owner for standalone whole-text calls. Standalone provider audio is not an evidence-authorized runtime speech path: runtime acts must still pass the Claim Gate.

Together uses its documented `session.created` readiness and JSON/base64 PCM protocol at `wss://api.together.ai/v1/realtime`. The previous explicit endpoint string in older config files must be updated. Documentation conformance and local socket tests are not paid-provider verification. Qwen keeps its whole-text/buffered behavior; no native text append support is asserted.

## Host language and response policies

Use `VoiceRuntime::build_with_policies(config, providers, RuntimePolicies)` to supply a pure shared `TextAnalyzer`, per-session slot-extractor factory, or trusted `SpeechRenderer`, and, with `framework`, the task and speculative tools (`with_task_tools`, `with_speculative_tools`). The library ships no business tools: a task spec that names a tool the host did not supply fails when the session's agent is built. `RuntimePolicies` is `#[non_exhaustive]`; build it from `RuntimePolicies::default()` and the `with_*` methods. Pass `None` for `providers` to retain built-in factories. Explicit host overrides take precedence; otherwise configured framework extraction and the declared fallback remain active. Direct sessions expose matching builder injection points and `try_build` validation. The library adds no locale negotiation or transport dependency.

The browser example's task spec returns `{"text", "outcome", "question"}` replies for a bounded reservation example. All example agent specs use `gpt-5.4-mini`. The task spec gives the model the server-local date each turn through an ai-agents `builtin` `datetime` context, so relative dates such as "tomorrow" resolve correctly; it also tells the model to read only the last four characters of a confirmation ID, which the demo reservation tool returns as `confirmation_suffix` so the model does not cut them itself. Once the agent has asked whether to book one option and the user agrees, the spec books it instead of asking a second time; it asks only for a detail that is still missing, such as the name. It books only a restaurant and time that a search returned, and it never promises to check or come back later, because nothing runs after a reply until the user speaks again. When it repeats a result from an earlier turn without calling the tool again, it names no outcome; naming one would fail the outcome check and replace the repeat with the failure text. Its state transitions use ai-agents `timing: parallel`: the transition is judged beside the reply instead of after it, so a turn without a transition costs one model call. The judge sees only the current user message and the runtime context, not the conversation history, so slots given over several turns may keep the agent in an earlier state. The executing-to-completed transition depends on the tool result and still runs after the reply, and the `completed` state does not regenerate the reply. In a live check, turns took about 1–5 s with this layout; with transitions judged after the reply they took about 2–9 s, and 3 of 9 turns ended with an empty reply after two chained transitions. Another spec can return plain text, which is spoken unchecked; it needs its own `speech.outcomes` rules and `tools.effects` declarations before it can report business outcomes. Changing `tts.language` changes the `speech_language` the task agent is asked to reply in, but not the automatic resources (backchannel, stop acknowledgement, failure, progress), `asr.locale`, or any grammar; set those together. With `tts.language` unset, the brief has no `speech_language` and the shipped spec tells the agent to reply in the user's language. A custom framework agent using `VoiceAgentHooks` must register tools through `hooks.bind_tool`; unbound results cannot authorize strict business speech. The adapter uses a stable session actor and an immutable per-turn sender token for execution provenance, not as user authentication.

## Environment variables

| Variable | Purpose | Default |
|---|---|---|
| `VOICE_BIND_ADDR` | Browser example bind address | `0.0.0.0:8080` |
| `RUST_LOG` | Tracing filter | `info` |
| `TOGETHER_API_KEY` | ASR | - |
| `DASHSCOPE_API_KEY` | TTS | - |
| `QWEN_VOICE_ID` | TTS voice; read from the variable named by `tts.voice_env` when `tts.voice` is empty | - |
| `CARTESIA_API_KEY` | Cartesia STT/TTS credential; selected by `asr.api_key_env` and `tts.api_key_env` in the full Cartesia sample | - |
| `CARTESIA_VOICE_ID` | Approved Cartesia voice; selected by `tts.voice_env` when `tts.voice` is empty | - |
| `OPENAI_API_KEY` | Task/Interaction LLM | - |
| `VOICE_WEB_DIR` | Override the browser example's client directory | `examples/browser/web` |
| `VOICE_KOREAN_FIXTURE` | Together/Cartesia STT contracts: consented 16 kHz mono PCM16 WAV; the Cartesia contract sends a fixture of at most 15 seconds twice. The server does not read it | - |

All keys remain in the Rust gateway and are not sent to the browser.

## Observability

- **JSONL event log**: `target/voice-traces/<date>/<session_id>/events.jsonl`
  - Records every partial revision, commit, tool lifecycle event, TTS chunk, and playback ACK.
  - By default, redacts transcript text, agent replies, and tool results (which can carry customer data such as a reservation name), retaining only character counts. Tool names and success flags are kept; tool arguments are never logged.
  - Stores original text, tool results, and SessionClosed text only when `include_sensitive_payloads: true`.
  - Error messages (`agent_failed`, `provider_error`, and the matching `tracing` warnings) are logged as produced, because they are needed for operation. Tools and providers should not put customer data in error text.
  - The writer drains the event channel until it closes, recording the final SessionClosed event.
- **Metrics**: `Metrics` counters and latency series (P50/P95/P99) are recorded within each session under `voice_*` names.
- When the browser WebSocket disconnects, the session supervisor is canceled and all tasks and TTS are cleaned up.
- When the ASR socket disconnects, a `provider_error` and internal `asr_stream_reset` are logged and reconnection uses exponential backoff from 250 ms to 8 s. Reconnect is fresh: pending PCM is discarded and audio during the disconnected interval may be lost; no lossless replay is claimed. Authentication/config rejections stop the recognizer without infinite retries, while enabled text injection remains usable.
- ASR input saturation reports `voice_asr_input_overflow_total`; client or supervisor output saturation terminates the session rather than blocking VAD/control processing indefinitely. Built-in client output waits at most one second; external hosts still own the capacity and consumption of their channels.
- A failed TTS reply discards its queued clauses; partial playback is aborted and conservatively settled before an independent act may replace the ledger. Every supervisor exit performs owned worker cleanup and main-turn resolution.

## Multiple sessions

A separate `RuntimeAgent` (task/speculative/interaction/deep) is created for each session. Conversation memory does not mix across clients, and root turns do not block each other, but session creation includes the cost of building agents. Only the host's tools, and whatever backend they wrap, are shared across the process; in the browser example that is the demo reservation store.

## What to watch in production event logs

```text
voice_user_turn_committed_total     Frequency of valid commits
voice_claim_rejected_total          Speech rejected for insufficient evidence (should be near 0)
voice_task_stale_result_dropped_total  Results dropped after correction (intentional)
voice_self_echo_suppressed_total    Self-echo defenses triggered
voice_tts_failed_total               TTS failures (should be 0; the queue recovers automatically)
voice_speech_aborted_total           Intentional aborts
voice_abort_deferred_total           Aborts deferred by Interruptibility::Never
voice_playback_ack_timeout_total     Missing playback ACKs (indicates client connection quality)
voice_backchannel_emitted_total      Proactive backchannels spoken
voice_deep_work_started_total        Host-initiated deep-work requests
voice_turn_resolved_total            Turns settled in memory using the heard result
voice_turn_heard_partially_total     Resolved turns heard only partially (interrupted, unplayed, or claim rejected)
voice_turn_discarded_total           Turns rolled back from memory (stale, failed, or superseded)
voice_task_superseded_total          Turns canceled when the next turn commits during inference
voice_semantic_timeout_total         Semantic interpretation requests that missed agents.interaction.timeout_ms
```

Watch logs for `semantic interpretation unavailable` or `timed out` (turns fall back to neutral timing), `reply not found in memory` (memory retains the generated reply) and `previous turn was never resolved` (missing resolution, a bug signal; recovery occurs after 10 seconds).

The `voice_false_interrupt` family of metrics is measured through scenario replay. For live sessions, observe it indirectly via the ratio of `playback_abort` to `user_turn_committed`.
