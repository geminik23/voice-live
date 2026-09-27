# voice-live

A Korean full-duplex voice interaction runtime and browser demo. It combines
existing streaming STT, an LLM, and streaming TTS to support simultaneous
listening and speaking, barge-in, backchannels, asynchronous tools, and progress
speech while reasoning.

Reasoning is delegated to [`ai-agents`](https://crates.io/crates/ai-agents), an
**optional dependency** behind the default `framework` feature. The
deterministic core builds and runs tests without it using
`--no-default-features`.

## Supported platforms

The library and demo server target **native Linux, Windows, and macOS**. Docker
and WSL are not required for native execution. A supported target is not the
same as a verified target: passing Rust tests does not establish browser audio,
physical microphone, or paid-provider behavior. See the
[platform validation guide](docs/testing.md#platform-validation) for evidence
and the [deployment guide](docs/deployment.md) for build requirements and
browser limitations.

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

# Start the keyless demo (text injection + mock ASR/TTS/agent).
cargo run -p full-duplex-demo -- --config full-duplex-demo/configs/voice-runtime.yaml
# Open http://localhost:8080 and click Start session.
# Enter Korean text in the input field to submit a user utterance.
```

The mock TTS produces **silent PCM**. To verify audio chunks and playback ACKs
without API keys, follow the [manual browser check](docs/deployment.md#manual-browser-playback-ack-check).

To use real providers, configure environment variables without committing
secrets. For Linux/macOS Bash or Zsh, or Windows Git Bash:

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

Then set `asr.provider: together` and `tts.provider: qwen` in
`full-duplex-demo/configs/voice-runtime.yaml` and run the same `cargo run`
command. The provider adapters still need paid contract testing before relying
on their wire formats.

Other environment variables: `VOICE_BIND_ADDR` (default `0.0.0.0:8080`),
`VOICE_WEB_DIR` (web root override), `RUST_LOG` (log filter), and
`VOICE_KOREAN_FIXTURE` (path to a 16 kHz mono PCM16 WAV for the Together
contract test). See the [environment variable reference](docs/deployment.md#environment-variables)
for details and path rules.

## Documentation

- [Architecture](docs/architecture.md) - runtime design and invariants
- [Modules](docs/modules.md) - library module reference
- [Configuration](docs/configuration.md) - `voice-runtime.yaml` reference
- [Protocol](docs/protocol.md) - browser WebSocket contract
- [Testing](docs/testing.md) - test layers and scenario DSL
- [Deployment](docs/deployment.md) - demo startup, environment, and Docker

## Core principles

1. An STT partial is a revision, not an append.
2. Do not enqueue partial requests on the same `RuntimeAgent`; the interaction
   and task agents use separate runtimes.
3. Keep transcript, action, and audible commit boundaries separate.
4. Never speak results from stale epochs.
5. Block factual and transactional claims without authoritative evidence.
6. Start barge-in with reversible ducking instead of immediate cancellation.
