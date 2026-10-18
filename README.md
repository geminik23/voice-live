# voice-live

A full-duplex voice interaction runtime and browser demo. It combines existing streaming STT, an LLM, and streaming TTS to support simultaneous listening and speaking, barge-in, backchannels, asynchronous tools, and progress speech while reasoning.

Reasoning is delegated to [`ai-agents`](https://crates.io/crates/ai-agents), an **optional dependency** behind the default `framework` feature. The deterministic core builds and runs tests without it using `--no-default-features`.

## Supported platforms

The library and demo server target **native Linux, Windows, and macOS**. Docker and WSL are not required for native execution. A supported target is not the same as a verified target: passing Rust tests does not establish browser audio, physical microphone, or paid-provider behavior. See the [platform validation guide](docs/testing.md#platform-validation) for evidence and the [deployment guide](docs/deployment.md) for build requirements and browser limitations.

## Layout

```text
voice-live/                   root package: runtime library
├── src/                      runtime core (events, epochs, claim gate, scenarios)
│   └── protocol.rs           client message types (transport-agnostic)
├── tests/                    virtual-time scenarios and provider contract tests
├── full-duplex-demo/         demo application (workspace member)
│   ├── src/main.rs           entry point
│   ├── src/gateway.rs        axum WebSocket transport adapter
│   ├── configs/              voice-runtime.yaml
│   ├── agents/               task and interaction agent YAML specs
│   └── web/                  browser client (WebSocket PCM)
├── docs/                     committed documentation and source of truth
├── AGENTS.md                 contribution, documentation, and architecture rules
└── Dockerfile
```

## Quick start

Run these commands from the repository root:

```sh
# Test the library only; no external API required.
cargo test

# Include the demo's tests.
cargo test --workspace

# Without OPENAI_API_KEY, start the demo with text injection and mock ASR/TTS/agent.
cargo run -p full-duplex-demo -- --config full-duplex-demo/configs/voice-runtime.yaml
# Open http://localhost:8080 and click Start session.
# Enter text in the input field to submit a user utterance.
```

The mock TTS produces **silent PCM**. The sample enables semantic LLM analysis: if `OPENAI_API_KEY` is present, recognized or injected text and heard conversation context can be sent to the model even when the task agent is mock, potentially incurring charges. For a keyless check, use a shell without that key and follow the [manual browser check](docs/deployment.md#manual-browser-playback-ack-check).

To use real providers, configure environment variables without committing secrets. For Linux/macOS Bash or Zsh, or Windows Git Bash:

```bash
export TOGETHER_API_KEY=...      # Nemotron streaming ASR
export DASHSCOPE_API_KEY=...      # Qwen realtime TTS
export QWEN_VOICE_ID=...         # TTS voice, as named by tts.voice_env
export OPENAI_API_KEY=...        # Task/Interaction LLM
```

For Windows PowerShell:

```powershell
$env:TOGETHER_API_KEY = "..."
$env:DASHSCOPE_API_KEY = "..."
$env:QWEN_VOICE_ID = "..."
$env:OPENAI_API_KEY = "..."
```

Then set `asr.provider: together` and `tts.provider: qwen` in `full-duplex-demo/configs/voice-runtime.yaml` and run the same `cargo run` command. The provider adapters still need paid contract testing before relying on their wire formats.

Other environment variables: `VOICE_BIND_ADDR` (default `0.0.0.0:8080`), `VOICE_WEB_DIR` (web root override), `RUST_LOG` (log filter), and `VOICE_KOREAN_FIXTURE` (path to a consented 16 kHz mono PCM16 WAV for the Together or Cartesia STT contract). See the [environment variable reference](docs/deployment.md#environment-variables) for details and path rules.

## Cartesia streaming STT and TTS

Cartesia manual streaming STT (`ink-whisper`) and native incremental TTS (`sonic-3.6`) are available through `asr.provider: cartesia` and `tts.provider: cartesia`, or host-injected `CartesiaStreamingAsr`/`CartesiaStreamingTts` factories. Use `full-duplex-demo/configs/voice-runtime.cartesia.yaml` for both providers. The optional `voice-runtime.cartesia-tts.yaml` keeps typed input for TTS-only diagnosis, and the default sample remains keyless inject ASR plus silent mock TTS.

Set `CARTESIA_API_KEY` and `CARTESIA_VOICE_ID` on the server using the shell syntax above, plus `OPENAI_API_KEY` for the sample's reservation task agent. The key is selected by both providers' `api_key_env`; the voice is selected by `tts.voice_env`. Do not put credentials or an account voice ID in committed configuration.

```sh
cargo run -p full-duplex-demo -- --config full-duplex-demo/configs/voice-runtime.cartesia.yaml
```

This setup sends microphone audio and conversation text externally and may incur charges. Premade TTS warmup is disabled. Offline wire/lifecycle and two-provider runtime tests pass. English provider-only streaming has also been checked against Cartesia with generated synthetic audio; [recorded results](docs/testing.md#english-provider-only-validation) are limited to that fixture. Korean, browser/microphone, and broader silence/overlap behavior remain unverified. The adapter uses ordered `flush_done` boundaries as an explicit integration assumption, not a vendor guarantee proven by local tests. See [Cartesia deployment](docs/deployment.md#cartesia-streaming-stt-and-tts), [STT settings and boundary assumptions](docs/configuration.md#cartesia-stt), and the [individual ignored contracts](docs/testing.md#cartesia-provider-regressions-and-paid-contracts) before live use.

## Replace speech providers

Hosts can pass `SpeechProviders { asr, tts }` to `VoiceRuntime::build_with_providers` without changing provider-name branches or adding transport dependencies. STT has independent PCM input and transcript output; TTS exposes buffered or native incremental text input with independent audio output. Legacy whole-text TTS can use the bounded buffered bridge. General agent replies still wait for authoritative Final and pass the Claim Gate; this API is not provisional-token speech. See [provider settings and migration](docs/configuration.md#injected-provider-settings-and-migration), [host integration](docs/deployment.md#host-supplied-speech-providers), and [deterministic provider tests](docs/testing.md#duplex-provider-regressions).

## Language-neutral policy and speech authorization

The library does not implicitly install a language grammar, slot heuristic, or spoken prompts. The demo enables an optional semantic LLM to interpret turn completion, corrections, interruption, and slot updates; if unavailable, timing and explicit controls remain active without silently selecting a language heuristic. A host can provide a per-session interaction brain or slot extractor, a pure `TextAnalyzer`, and a trusted `SpeechRenderer` through `RuntimePolicies`. The reservation examples supply Korean automatic resources (backchannel, stop acknowledgement, failure, progress), not an implicit language policy. The task agent is told the configured `tts.language` and writes its replies in it; the runtime does not detect languages or prove model/voice support.

In the default `speech.mode: strict`, the task agent writes its own reply text. A reply that reports a business outcome names it, and the runtime speaks it only if the tools executed in that turn confirm the outcome; a failed write in the turn replaces the reply with the configured failure text. Replies that name no outcome are spoken unchecked, so this is not a guarantee of arbitrary natural-language truth. Existing free-text integrations can explicitly select `legacy`, which retains the older heuristic limitations. See [replies, outcomes, and migration](docs/configuration.md#strict-replies-and-outcomes) and [authorization boundaries](docs/architecture.md#claim-gate). The keyless sample still exercises the complete pipeline using `speech.mock_reply` and silent mock audio.

## Documentation

- [Architecture](docs/architecture.md) - runtime design and invariants
- [Modules](docs/modules.md) - library module reference
- [Configuration](docs/configuration.md) - `voice-runtime.yaml` reference
- [Protocol](docs/protocol.md) - browser WebSocket contract
- [Testing](docs/testing.md) - test layers and scenario DSL
- [Deployment](docs/deployment.md) - demo startup, environment, and Docker

## License

Copyright 2026 Jaemin Kim.

The `voice-live` library and `full-duplex-demo` are licensed under the [Apache License, Version 2.0](LICENSE). Third-party dependencies retain their respective licenses.

## Core principles

1. An STT partial is a revision, not an append.
2. Do not enqueue partial requests on the same `RuntimeAgent`; the interaction and task agents use separate runtimes.
3. Keep transcript, action, and audible commit boundaries separate.
4. Never speak results from stale epochs.
5. Block factual and transactional claims without authoritative evidence.
6. Start barge-in with reversible ducking instead of immediate cancellation.
