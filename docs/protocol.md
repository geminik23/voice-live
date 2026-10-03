# WebSocket protocol

Transport contract between the browser client (`full-duplex-demo/web/index.html`)
and the Rust gateway.

The message types live in the library (`src/protocol.rs`), while the WebSocket
adapter lives in the demo (`full-duplex-demo/src/gateway.rs`). The library is
transport-agnostic.

## Overview

- Endpoint: `GET /api/voice/ws` (WebSocket upgrade)
- Static files: `/` → `AppState::web_dir` (the demo resolves this relative to the
  config file; `VOICE_WEB_DIR` can override it)
- Audio input: the microphone's **actual** sample rate, 20 ms frames, mono
- Audio output: mono PCM16 at the response's actual `speech_started.sample_rate` (built-in default 24 kHz)
- All API keys stay on the server and are never sent to the browser.

## Client → server

### Binary: audio frame

```text
[ u64 LE sequence ][ i16 LE PCM ... ]
```

Frames are 20 ms long, with the sample count determined by the actual rate (48 kHz → 960 samples → 8 + 1920 bytes; 44.1 kHz → 882 samples → 8 + 1764 bytes). The sequence increases with each frame; the runtime tracks gaps/reordering and drops late or duplicate frames. A large client frame gap (>25 frames) is reported as an input error, the current PCM frame is not admitted to ASR, and an active recognizer reconnects fresh after local VAD processing. Provider disconnect and ASR queue saturation also reconnect fresh, not with generic PCM replay; the disconnected interval may be lost.

### JSON control (`tag = "type"`)

```jsonc
// 1. Start the session (send before the first frame).
//    input_sample_rate must be the measured AudioContext.sampleRate.
//    Browsers can ignore the requested rate; the server chooses a resampler
//    based on this value. An incorrect value silently breaks recognition.
{ "type": "session_start", "input_sample_rate": 48000, "channels": 1 }

// 2. Client VAD (optional, advisory; server energy VAD makes the decision)
{ "type": "local_vad", "state": "speech", "probability": 0.92 }

// 3. Chunk playback ACK (sent by the playback thread after actual playback)
{ "type": "playback_chunk_completed",
  "speech_id": "uuid", "speech_epoch": 3,
  "sequence": 2, "played_samples": 48000 }

// 4. Entire utterance finished playing
{ "type": "playback_completed", "speech_id": "uuid", "speech_epoch": 3 }

// 5. Report the amount actually played on interruption
{ "type": "playback_interrupted",
  "speech_id": "uuid", "speech_epoch": 3, "played_samples": 6240 }

// 6. Development text injection (requires dev.allow_text_injection)
{ "type": "inject_user_text", "text": "토요일 저녁 7시로 예약해줘" }

// 7. End the session
{ "type": "session_stop" }
```

`played_samples` is the basis for `AudibleLedger`'s calculation of actual
listening time. Clients must report only the amount they actually played.

## Server → client

### Binary: TTS audio chunk

```text
[ u64 LE sequence ][ i16 LE PCM ... ]
```

`speech_started` arrives first, followed by the chunks. Always schedule chunks
in order. Send `playback_completed` only after `speech_done` has arrived **and**
the playback queue is empty; if the queue emptied earlier, send it when
`speech_done` arrives.

### JSON control

```jsonc
// Utterance starts (once, just before its first chunk)
{ "type": "speech_started",
  "speech_id": "uuid", "speech_epoch": 3, "sample_rate": 24000 }

// Finished sending the utterance's final chunk
{ "type": "speech_done", "speech_id": "uuid" }

// Barge-in stage 1: immediately reduce volume (reversible)
{ "type": "playback_duck", "gain": 0.15, "fade_ms": 60 }

// Identified as a backchannel: restore the original volume
{ "type": "playback_resume", "gain": 1.0, "fade_ms": 60 }

// Actual interruption: stop playback immediately
{ "type": "playback_abort", "new_speech_epoch": 4, "reason": "hard_stop" }

// Discard the entire queue
{ "type": "playback_clear" }

// Text injection rejected (dev.allow_text_injection disabled or session unknown)
{ "type": "injection_rejected", "reason": "dev.allow_text_injection is disabled" }

// Session ended
{ "type": "session_stopped" }
```

## Client state machine

```text
on speech_started   → update current_speech, played_samples = 0
on binary           → schedule in order, send chunk_completed ACK after each chunk finishes playing
on speech_done      → set doneSignaled; if the queue is already empty, send playback_completed
on playback_abort   → discard queue + send playback_interrupted with samples played so far
on playback_duck    → gain ramp (fade_ms/3 time constant)
on playback_resume  → restore gain ramp
```

Unless the server plays audio on the client's behalf, client playback is the
only source of truth for actual listening time. Reliable ACKs keep Audible
Commit and latency metrics accurate.

### Server-side protection against missing ACKs

`SpeechState` advances only on valid client ACKs. After the first nonempty audio, playback inactivity for `speech.playback_ack_timeout_ms` (default 15 seconds) invalidates the speech epoch, sends Abort, and settles using only progress already confirmed (`voice_playback_ack_timeout_total`). Incoming provider audio does not refresh this timer. Timeout settlement can retain a conservative Partial or None; it is not proof of Full playback.

Synthesis failure after audio also sends `playback_abort`, never `speech_done`. The runtime waits at most `speech.playback_stop_ack_timeout_ms` (default 1000 ms) for the matching interrupted ACK before conservative settlement; ordinary successor speech waits so it cannot overwrite the ledger. A new committed user turn can instead fast-settle at its commit boundary. Old/duplicate ACKs cannot refresh a successor's watchdog, replay settlement, or rewrite its context. An interrupted ACK remains valid for an explicitly stopping speech even after an epoch bump. These are semantic protections; the JSON and binary wire formats are unchanged.
