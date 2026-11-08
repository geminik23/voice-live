# voice-live

A Rust runtime for full-duplex voice interaction, inspired by GPT-Live and built as a use case for `ai-agents`. It combines streaming speech recognition, an LLM agent, and streaming speech synthesis so an assistant can listen while speaking. [ai-agents on GitHub](https://github.com/geminik23/ai-agents) · [OpenAI's GPT-Live engineering write-up](https://openai.com/index/continuous-voice-interaction-with-gpt-live/)

**Experimental (0.1).** Planned work includes reducing response latency, improving interruption handling, and validating longer sessions. The public API may change between minor releases. [Known limitations](#known-limitations)

## Quick start

Run both examples from the repository root with Rust 1.88 or later.

### Terminal

```sh
cargo run --example quickstart
```

The terminal example sends a line of text and acknowledges a silent reply. It runs without API keys. [Quickstart source](examples/quickstart.rs)

### Browser demo

```sh
cargo run -p browser-example -- --config examples/browser/configs/en/voice-runtime.yaml
```

Open `http://localhost:8080`, select **Start session**, and allow microphone access. The in-memory restaurant booking demo uses typed input and silent audio in this configuration. Use a shell without `OPENAI_API_KEY` for a mock-only run. With the key set, the demo can send conversation text to OpenAI. [Korean configuration](examples/browser/configs/ko/voice-runtime.yaml)

The Cartesia configs enable live microphone input and speech output. They send audio and text to external providers and can incur charges. [Live audio setup](docs/deployment.md#cartesia-streaming-stt-and-tts)

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
