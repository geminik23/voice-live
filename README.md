# voice-live

A Rust runtime for full-duplex voice interaction, inspired by GPT-Live and built as a use case for `ai-agents`. It combines streaming speech recognition, an LLM agent, and streaming speech synthesis so an assistant can listen while speaking. [ai-agents on GitHub](https://github.com/geminik23/ai-agents) · [OpenAI's GPT-Live engineering write-up](https://openai.com/index/continuous-voice-interaction-with-gpt-live/)

**Experimental (0.1).** Planned work includes reducing response latency, improving interruption handling, and validating longer sessions. The public API may change between minor releases. [Known limitations](#known-limitations)

## Quick start

Run the examples from the repository root with Rust 1.88 or later.

### Browser demo (Cartesia)

The live browser demo uses **Cartesia Ink Whisper STT + Sonic TTS**, with OpenAI for the task agent and semantic interpretation. It demonstrates an in-memory restaurant booking flow, not a connection to real restaurants.

Before starting, set these environment variables in the same shell that will run Cargo:

- `CARTESIA_API_KEY`: your Cartesia API key for STT and TTS.
- `CARTESIA_VOICE_ID`: an approved voice available to your Cartesia account for the selected session language.
- `OPENAI_API_KEY`: your OpenAI API key for the configured agents.

For local testing, bind to loopback so other devices cannot connect to the example server.

Windows PowerShell:

```powershell
$env:CARTESIA_API_KEY = "<your-cartesia-api-key>"
$env:CARTESIA_VOICE_ID = "<your-cartesia-voice-id>"
$env:OPENAI_API_KEY = "<your-openai-api-key>"
$env:VOICE_BIND_ADDR = "127.0.0.1:8080"
```

Bash, Zsh, or Git Bash:

```sh
export CARTESIA_API_KEY="<your-cartesia-api-key>"
export CARTESIA_VOICE_ID="<your-cartesia-voice-id>"
export OPENAI_API_KEY="<your-openai-api-key>"
export VOICE_BIND_ADDR="127.0.0.1:8080"
```

If the variables are already set, keep their existing values; do not replace them with the placeholders above. The server does not load `.env` files automatically. Variables set in another terminal are not inherited by an already running editor shell.

Run **one** configuration for the conversation language:

```sh
# English
cargo run -p browser-example -- --config examples/browser/configs/en/voice-runtime.cartesia.yaml
```

```sh
# Korean
cargo run -p browser-example -- --config examples/browser/configs/ko/voice-runtime.cartesia.yaml
```

Open `http://localhost:8080`, select **Start session**, allow microphone access, and speak a complete request in the selected language. Headphones are recommended for an initial check. Confirm that a spoken request produces an audible reply, then try speaking over a reply to check interruption. Response and stop recognition can take several seconds; very short Korean replies may be missed. Close the session when finished and stop the server with Ctrl+C.

**External calls and charges:** microphone audio is streamed to Cartesia after session start, conversation text is sent to OpenAI, and reply text is sent to Cartesia TTS. Streaming can include silence, even while you use typed input. These calls can incur charges; confirm account/voice access and transmission approval before starting. The live configuration requires the keys above and does not silently fall back to a mock task agent. API keys stay on the server; do not put them in committed YAML or browser code.

Windows Edge has recorded microphone/speaker validation. Other browser/device combinations and HTTPS deployments need their own checks; remote microphone access requires HTTPS/WSS. [Live setup and TTS-only diagnosis](docs/deployment.md#cartesia-streaming-stt-and-tts) · [Provider tests and validation limits](docs/testing.md#cartesia-provider-regressions-and-paid-contracts)

### Terminal (no API keys)

```sh
cargo run --example quickstart
```

The terminal example sends a line of text and acknowledges a silent reply without provider calls. [Quickstart source](examples/quickstart.rs)

For a browser check without live STT/TTS, use the [English](examples/browser/configs/en/voice-runtime.yaml) or [Korean](examples/browser/configs/ko/voice-runtime.yaml) inject/mock configuration instead. Keep `OPENAI_API_KEY` unset for a mock-only browser run; mock speech providers alone do not prevent LLM calls. [Keyless browser setup](docs/deployment.md#running-locally-no-api-keys-required)

## Runtime behavior

- **Barge-in.** The runtime lowers playback volume as soon as it detects user speech, then decides whether to resume or stop playback.
- **Playback-based memory.** Client playback reports determine how much of the final assistant reply is kept in memory.
- **Business outcome checks.** The default renderer checks named outcomes against the current turn's tool records. An unconfirmed write blocks the reply.

## Known limitations

- Replies can take several seconds. In one live test, playback stopped 1.5–2.7 seconds after the recognizer picked up a question spoken over the assistant.
- In Korean tests, Cartesia Ink Whisper sometimes missed short replies such as "네" or "응".
- A reply without an `outcome` can claim a business result even when no tool ran.
- The browser example relies on browser echo cancellation. The runtime currently has no server-side echo cancellation, WebRTC, or Opus support.
- Live microphone and speaker testing has covered Windows with Edge. HTTPS deployments still need live validation. [Platform validation](docs/testing.md#platform-validation)

## Integration

Hosts connect `VoiceRuntime` session channels to their own transport. The default `framework` feature enables `ai-agents` integration. [Agent configuration](docs/configuration.md#framework-integration) · [Host tools](docs/deployment.md#host-language-and-response-policies) · [Speech providers](docs/configuration.md#choosing-a-provider)

## Documentation

[Architecture](docs/architecture.md) · [Modules](docs/modules.md) · [Configuration](docs/configuration.md) · [Protocol](docs/protocol.md) · [Testing](docs/testing.md) · [Deployment](docs/deployment.md)

## License

Copyright 2026 Jaemin Kim. Licensed under the [Apache License, Version 2.0](LICENSE).
