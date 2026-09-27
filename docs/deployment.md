# Running and deployment

## Supported platforms and build prerequisites

The `voice-live` library and `full-duplex-demo` server target **native execution on Linux, Windows, and macOS**. They should not depend on a particular developer's OS, shell, or absolute paths. This applies both to the default build with the `framework` feature and to the core build without it. The library is a server-side Rust runtime; the browser is the demo client. This support scope does not imply that the Rust library supports WASM.

- All platforms: install stable Rust with rustup and the native build tools for your target. `cargo` and `rustc` must be available on your shell's PATH.
- Linux: install your distribution's build tools, including a C/C++ compiler and linker.
- Windows: for the MSVC target, install the Visual Studio Build Tools C++ components and Windows SDK. You can run directly from PowerShell; Git Bash, WSL, and Docker are not required. Shell-specific PATH setup belongs to the development environment, not hardcoded library configuration.
- macOS: install native build tools such as Xcode Command Line Tools.
- Node.js is used to validate the browser script; it is not required to run the Rust server.

Distinguish supported targets from actual validation results. Check OS, CPU architecture, Rust target, and browser separately; see [platform validation](testing.md#platform-validation) for current evidence and procedures.

## Running locally (no API keys required)

Run this command from the repository root; it is the same in Bash, Zsh, and PowerShell.

```bash
cargo run -p full-duplex-demo -- --config full-duplex-demo/configs/voice-runtime.yaml
# Open http://localhost:8080 → Start session
```

Paths are resolved as follows:

- Relative paths passed to `--config` are relative to the **process working directory**. From another directory, supply the correct relative or absolute path.
- Relative `agents.*.spec` paths loaded with `VoiceRuntimeConfig::from_file` are relative to the **config file's directory**. `from_yaml_str` does not adjust these paths.
- The demo's default web root is `../web` relative to the config directory. A relative path set via `VOICE_WEB_DIR` is relative to the working directory.
- Relative `observability.event_log_dir` paths are also relative to the working directory, independent of Cargo's build output location.

Use portable relative paths such as `../agents/reservation.yaml` in shared YAML. `/` separators also work on Windows. Configure OS-specific absolute paths on each host instead of fixing them in the repository. Quote CLI paths containing spaces and match filename case exactly for case-sensitive filesystems such as those commonly used on Linux.

The sample config uses `asr.provider: inject` and `dev.allow_text_injection: true`.

- Text typed in the input box at the bottom of the page becomes the user's utterance. It follows the same commit → agent → claim gate → TTS → playback path as speech.
- With the microphone enabled, you can also check full-duplex behavior such as duck/resume/abort using mock TTS.
- `allow_text_injection` works with **all** ASR providers, so you can use `provider: together` for real recognition while reproducing scenarios with text.

### Browser requirements and current limitations

The browser must support WebSocket, Web Audio, AudioWorklet, and `getUserMedia`. Microphone access requires browser and OS permissions and a secure context. For local development, use `http://localhost:8080`; for access from another device, use HTTPS/WSS. Currently, `Start session` requests microphone permission even when using only text injection. The sample `mock` TTS emits silent PCM, so it cannot verify that natural speech is audible.

The web client sets `ws.binaryType = 'arraybuffer'` and decodes the `ArrayBuffer` directly. `node --test full-duplex-demo/tests/web_playback.test.cjs` checks the real inline script's WebSocket initialization, binary PCM delivery, and ACK after simulated playback completes. It covers more than `node --check` (syntax only), but does not guarantee actual audio output or microphone permission behavior in a browser/OS combination.

<a id="manual-browser-playback-ack-check"></a>

### Manual browser playback ACK check without API keys

1. From the repository root, run the `cargo run -p full-duplex-demo -- --config ...` command above. Without real keys configured, the sample uses inject ASR, mock TTS, and a mock agent. If `OPENAI_API_KEY` is set, the task agent may use a real LLM; use a separate shell without the key for a key-free check.
2. On the same computer, open `http://localhost:8080` in a browser, then open developer tools to **Console** and **Network → WS → `/api/voice/ws` → Messages/Frames**.
3. Click **Start session** and allow microphone access. This demo opens the microphone even when using only text injection. A remote browser not on `localhost` requires HTTPS/WSS.
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

In `full-duplex-demo/configs/voice-runtime.yaml`:

```yaml
asr:
  provider: together
tts:
  provider: qwen
agents:
  interaction:
    enabled: true    # Optional LLM judgment for ambiguous cases
  task:
    spec: ../agents/reservation.yaml
```

First verify the provider adapters' wire fields with the paid contract tests:

```bash
cargo test --test provider_contracts -- --ignored
```

For an unverified provider combination, start diagnosis with the `provider_error` events in the logs and `target/voice-traces/<date>/<session>/events.jsonl`.

## Environment variables

| Variable | Purpose | Default |
|---|---|---|
| `VOICE_BIND_ADDR` | Bind address | `0.0.0.0:8080` |
| `RUST_LOG` | Tracing filter | `info` |
| `TOGETHER_API_KEY` | ASR | - |
| `DASHSCOPE_API_KEY` | TTS | - |
| `QWEN_VOICE_ID` | TTS voice; read from the variable named by `tts.voice_env` when `tts.voice` is empty | - |
| `OPENAI_API_KEY` | Task/Interaction LLM | - |
| `VOICE_WEB_DIR` | Override the browser client directory | `../web` relative to the config file |
| `VOICE_KOREAN_FIXTURE` | Together contract tests only: path to a consented 16 kHz mono PCM16 WAV; the server does not read it | - |

All keys remain in the Rust gateway and are not sent to the browser.

## Docker

Docker is an **optional Linux-container deployment method**, not a replacement for native multiplatform support. The current Dockerfile uses Linux builder and runtime images. It can run on Windows/macOS with a Linux container engine such as Docker Desktop or a remote Linux engine. A successful Docker build does not validate native Windows/macOS builds or browser/microphone behavior; the Dockerfile itself also requires a separate build check.

Use one-line commands to avoid shell-specific line-continuation syntax.

```sh
docker build -t full-duplex-demo .
docker run -p 8080:8080 -e TOGETHER_API_KEY -e DASHSCOPE_API_KEY -e QWEN_VOICE_ID -e OPENAI_API_KEY full-duplex-demo
```

Microphone permissions require HTTPS in production, so terminate TLS at a reverse proxy. WebSocket PCM is sufficient before a switch to WebRTC.

## Observability

- **JSONL event log**: `target/voice-traces/<date>/<session_id>/events.jsonl`
  - Records every partial revision, commit, tool lifecycle event, TTS chunk, and playback ACK.
  - By default, redacts transcript/agent text, retaining only character counts.
  - Stores original text and SessionClosed text only when `include_sensitive_payloads: true`.
  - The writer drains the event channel until it closes, recording the final SessionClosed event.
- **Metrics**: `Metrics` counters and latency series (P50/P95/P99) are recorded within each session under `voice_*` names.
- When the browser WebSocket disconnects, the session supervisor is canceled and all tasks and TTS are cleaned up.
- When the ASR socket disconnects, a `provider_error` event is logged and reconnection uses exponential backoff from 250 ms to 8 s, replaying roughly the most recent 1.5 seconds of PCM.

## Multiple sessions

A separate `RuntimeAgent` (task/speculative/interaction/deep) is created for each session. Conversation memory does not mix across clients, and root turns do not block each other, but session creation includes the cost of building agents. Only the reservation backend (`ReservationStore`) is shared across the process.

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
```

Watch logs for `reply not found in memory` (memory retains the generated reply) and `previous turn was never resolved` (missing resolution, a bug signal; recovery occurs after 10 seconds).

The `voice_false_interrupt` family of metrics is measured through scenario replay. For live sessions, observe it indirectly via the ratio of `playback_abort` to `user_turn_committed`.
