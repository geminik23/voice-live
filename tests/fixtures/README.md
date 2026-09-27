# Provider contract audio fixture

The paid Together ASR contract test intentionally does not commit recorded
speech to the repository. Supply a short Korean utterance as a signed 16-bit,
16 kHz, mono PCM WAV and point `VOICE_KOREAN_FIXTURE` to it:

```bash
export TOGETHER_API_KEY=...
export VOICE_KOREAN_FIXTURE=/absolute/path/korean-reservation-16k.wav
cargo test \
  --test provider_contracts \
  together_nemotron_korean_contract \
  -- --ignored --nocapture
```

A suitable sentence is: "토요일 저녁 일곱 시에 네 명 예약해 주세요."

Recordings may contain biometric and personal data. Obtain consent and keep
fixtures outside version control unless they are explicitly licensed for use.
