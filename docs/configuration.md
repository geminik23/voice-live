# Configuration reference (voice-runtime.yaml)

All fields are optional and use defaults when omitted. The YAML below illustrates the Korean demo, not the library's neutral linguistic defaults. In a minimal configuration, text rules, automatic speech resources, `asr.locale`, and `tts.language` are empty. Task-agent replies are spoken as the model writes them; outcome rules only decide whether a named business outcome may be claimed. Secrets are not stored in this file; `api_key_env` looks them up only from environment variables on the built-in provider path.

```yaml
version: 1

session:
  control_tick_ms: 40        # deterministic tick interval

audio:
  input_sample_rate_hz: 16000   # sample rate sent to ASR
  output_sample_rate_hz: 24000  # TTS playback sample rate
  frame_ms: 20

asr:
  provider: inject          # together | cartesia | inject | mock
  model: nvidia/nemotron-3.5-asr-streaming-0.6b
  endpoint: wss://api.together.ai/v1/realtime
  api_key_env: TOGETHER_API_KEY
  chunk_ms: 160
  manual_commit: false     # when true, server VAD controls endpointing and admits a FIFO provider commit after trailing PCM
  commit_delay_ms: 300     # with manual_commit, audio sent after local speech ends before the commit
  idle_commit_ms: 2000     # with manual_commit, a commit every this much silence; 0 disables
  locale: ko-KR            # no default; required by the together and cartesia providers
  open_timeout_ms: 10000
  write_timeout_ms: 2000
  input_buffer_ms: 1000
  cartesia:                 # used only by the Cartesia STT factory
    api_version: "2026-08-14"
    finalize_timeout_ms: 2000
    finish_timeout_ms: 5000
    max_transcript_bytes: 65536
  partials:
    enabled: true
    stable_after_ms: 320   # only prefixes unchanged for this long are stable
    stable_revisions: 2    # must match for N consecutive revisions to be stable

tts:
  provider: mock           # qwen | cartesia | mock
  model: qwen3-tts-flash
  endpoint: wss://dashscope.aliyuncs.com/api/v1/inference/qwen3-tts/realtime
  api_key_env: DASHSCOPE_API_KEY
  voice: ""                # if empty, read from voice_env
  voice_env: QWEN_VOICE_ID # voice IDs are account-specific; do not commit one to config
  sample_rate_hz: 24000
  chunk_ms: 60
  language: Korean         # no default; required by qwen and cartesia; also the agent's reply language
  open_timeout_ms: 10000
  request_timeout_ms: 30000
  max_input_bytes: 65536
  cartesia:                 # used only by the Cartesia TTS factory
    api_version: "2026-08-14"
    max_buffer_delay_ms: 0
  premade:
    enabled: true
    phrases: ["네", "네, 확인했어요."]   # pre-synthesize at startup for immediate playback

turn_control:
  soft_silence_ms: 500      # start evaluating endpointing after this silence
  hard_silence_ms: 1100     # safety-net commit
  require_semantic_completion: true
  barge_in:
    duck_immediately: true
    duck_gain: 0.15
    duck_fade_ms: 60
    confirm_ms: 240         # wait after ducking before classification
  backchannel:
    enabled: true
    minimum_interval_ms: 2500
    maximum_per_user_turn: 2
  slot_fallback: neutral    # explicit compatibility heuristics are optional
  answer_min_silence_ms: 320   # silence threshold for committing a short answer after a question

agents:
  interaction:
    enabled: true           # sample setting; library default is false
    spec: ../agents/voice-interaction.yaml
    minimum_interval_ms: 250
    timeout_ms: 1500        # library default 900; hosted models took 1.1-1.6 s
  task:
    spec: ../agents/reservation.yaml
    timeout_ms: 30000
  deep:
    enabled: false
    spec: ../agents/deep-worker.yaml
    timeout_ms: 60000
  speculative:
    enabled: false          # when true, run a read-only speculative task in a separate runtime instance
    spec: ../agents/reservation-readonly.yaml
    allowed_tools: [search_availability]
    minimum_intent_confidence: 0.85
    maximum_parallel_tasks: 1
    timeout_ms: 5000
    allow_pure_read: true

speech:
  hard_stop_ack_text: "네, 멈췄어요."
  playback_ack_timeout_ms: 15000  # release the speech queue if no playback ACK arrives; 0 disables this
  playback_stop_ack_timeout_ms: 1000 # bounded interrupted ACK wait after Abort
  audible_history_max_clauses: 64 # recent clauses kept by the audible ledger for echo detection and summaries
  progress_templates:
    search_availability:         # tool name is the key
      text: "조건에 맞는 곳을 확인하고 있어요."
      minimum_expected_latency_ms: 1200   # 0 omits the progress message
      expire_after_ms: 4000

tools:
  effects:
    search_availability: pure_read
    reserve_restaurant: transactional_write
    # IdempotentWrite is also supported; only TransactionalWrite creates an Action Commit

vad:
  onset_frames: 3           # consecutive frames (about 60 ms at 20 ms per frame)
  offset_frames: 12         # about 240 ms
  onset_ratio: 4.0          # multiplier of noise_floor
  offset_ratio: 2.0
  # The floor ignores digital silence (a muted or gated input). While speech is
  # on, a second whose quietest frame stays above the offset level raises the
  # floor to that level, so a changed room noise cannot hold speech on.

echo:
  enabled: true
  similarity_threshold: 0.82
  min_chars: 4

observability:
  event_log: true
  event_log_dir: target/voice-traces
  include_sensitive_payloads: false  # default: redact transcript, reply, and tool-result text

dev:
  allow_text_injection: true   # allow the inject_user_text client message
```

## Why the default configuration runs immediately

The sample configuration runs without API keys.

- `asr.provider: inject` — a silent recognizer plus a text-injection channel
- `dev.allow_text_injection: true` — the browser text box acts as the user's utterance
- `tts.provider: mock` — deterministic silent PCM

When `allow_text_injection` is enabled, the runtime owns a per-session injection queue independently of the recognizer connection. Typed finals therefore keep flowing during ASR open failures and reconnects, without crossing between clients. `InjectableAsr` remains available as a standalone decorator; the runtime no longer ties injection delivery to that decorator's open.

## Choosing a provider

| Provider | Purpose | Requirements |
|---|---|---|
| `mock` (ASR/TTS) | Offline demo and pipeline checks | None |
| `inject` (ASR) | Development text injection: the browser text box acts as the user's utterance | `dev.allow_text_injection` |
| `together` (ASR) | Live streaming recognition | `TOGETHER_API_KEY` |
| `qwen` (TTS) | Live speech synthesis | `DASHSCOPE_API_KEY`, `QWEN_VOICE_ID` (or `tts.voice`) |
| `cartesia` (ASR) | Ink Whisper manual streaming recognition | Explicit Cartesia endpoint/model/env; provider-supported language selector, 16 kHz, `manual_commit: true`, `CARTESIA_API_KEY` |
| `cartesia` (TTS) | Native incremental Sonic synthesis | Explicit Cartesia endpoint/model/env settings; `CARTESIA_API_KEY`, `CARTESIA_VOICE_ID` (or `tts.voice`) |

Unknown provider names fail the built-in runtime build; they do not silently fall back to `mock`. The injected builder uses the host's factories instead of resolving these names.

Validate each real provider with its individual ignored contract only after credential, cost, and external-transmission approval. Do not run all ignored tests as a keyless check. The Together and Cartesia STT contracts require a consented 16 kHz mono PCM WAV supplied through `VOICE_KOREAN_FIXTURE`; the Cartesia TTS contract sends synthetic Korean text and requires an approved voice. See [provider testing](testing.md#cartesia-provider-regressions-and-paid-contracts).

## Cartesia STT

With `manual_commit: true`, the runtime does not commit the moment local speech ends. It keeps sending audio for `asr.commit_delay_ms` (default 300) and then commits. In a live check, Ink Whisper returned an empty transcript for a short Korean word when the finalize arrived 240 ms after the word ended, and the correct word at 340 ms or later. If speech resumes inside the window, it stays in the same utterance and gets one finalize instead of two. Set `0` to commit immediately. The delay does not hold the turn: turn commit still follows `turn_control`.

While nobody speaks, the runtime also commits every `asr.idle_commit_ms` (default 2000), so the recognizer never holds a long stretch of silence. In a live check, after about 14 s of silence Ink Whisper split the next utterance at a short pause and returned an empty transcript for its end ("...강남에서" without "두 명이요") in 4 of 6 utterances; with idle commits every 2 s, 9 of 10 were complete, and 1 s was not better. An idle commit is not sent while local speech is on, while an onset is being confirmed, or while a speech commit is pending, and its empty Final is not forwarded as an utterance. Set `0` to disable.

Use `full-duplex-demo/configs/voice-runtime.cartesia.yaml` for Cartesia STT and TTS together. Set `asr.provider: cartesia`, endpoint `wss://api.cartesia.ai/stt/websocket`, model `ink-whisper`, `api_key_env: CARTESIA_API_KEY`, `manual_commit: true`, locale `ko-KR`, and `audio.input_sample_rate_hz: 16000`. The session selector is normalized to a primary language code, for example `fr-CA → fr`, `zh-Hant-TW → zh`, and `yue → yue`. There is no runtime language-membership allowlist. Unsupported model, rate, or automatic commit settings are still rejected rather than silently substituted. Provider-name changes alone retain Together defaults and are not a valid migration.

| Key | Default | Validation and use |
|---|---:|---|
| `asr.cartesia.api_version` | `2026-08-14` | Currently the only accepted version, sent in `Cartesia-Version` |
| `asr.cartesia.finalize_timeout_ms` | 2000 | Positive bounded duration; each finalize has a write deadline, then an independent absolute acknowledgement deadline from write completion |
| `asr.cartesia.finish_timeout_ms` | 5000 | Positive bounded duration from the first successful finish admission, including queued PCM, pending flushes, close write, and done |
| `asr.cartesia.max_transcript_bytes` | 65536 | Positive UTF-8 byte limit for confirmed prefix plus mutable tail and each delivered transcript |

The factory validates its own settings and credentials; unused Cartesia settings do not reject a host-injected provider. Empty nested settings use serde defaults. Endpoints must omit userinfo and query parameters; the adapter owns the model/encoding/sample-rate/language query. WSS is required outside loopback. Authentication and successful WebSocket upgrade establish send-readiness; no application-level ready event is invented.

PCM is binary mono signed little-endian PCM16. `asr.chunk_ms` sets the wire message size and is capped at 100 ms: the 20 ms frames the runtime receives are batched into messages of that size, a commit first sends whatever audio is still held, and a partial message is sent after waiting one chunk duration, so audio is never held longer than that. In a live check, Ink Whisper returned an empty transcript for a short Korean word sent as 20 ms messages and the right word for the same audio sent as 100 ms messages. The adapter does not insert silent audio or drop supplied silence. It paces sends at approximately real time, rebasing after substantial lateness rather than replaying a large backlog as a burst. Input samples remain budgeted throughout writer ownership, conversion, and pacing. At most 64 command slots (or the smaller host limit) cover queued/writer-held audio and outstanding finalize markers. Pending markers retain their slots until acknowledgement. Finish is reserved independently of queue saturation. The event queue holds at most 16 transcript events, each limited by `max_transcript_bytes`; control and terminal storage are separate and bounded. WebSocket frames/messages are limited to 262144 bytes.

Confirmed transcript deltas append verbatim; mutable tails replace, never append. No delta trimming or inserted spaces occur inside the adapter. Full replacement Partial snapshots have connection-monotonic revisions, and one full confirmed Final is emitted per `flush_done`, including an empty boundary when necessary. `request_id` identifies the connection, not an utterance. Local utterance IDs advance only at boundaries; runtime remapping keeps identities monotonic across reconnects. Silence auto-finalization confirms prefix text without independently ending the runtime utterance. Already selected partial-based turns are not rewritten by a late Final.

**Integration assumption:** pending finalizes and transcript boundaries are attributed FIFO: all text for a flush must precede its acknowledgement, and successor text must follow it. PCM continues while acknowledgements are pending; there is no per-flush stop-and-wait pause. Official schema/SDK descriptions support the manual command flow but do not completely establish multiple-outstanding-finalize and silence-race semantics for Ink Whisper. Local tests verify adapter behavior under that ordering, not the real server guarantee. The ignored contract and live tests must validate it before relying on the combination. Wrong connection IDs, unsolicited acknowledgements, missing acknowledgements, malformed events, and budget failures terminate the stream; an indistinguishable duplicate acknowledgement or semantic misattribution cannot always be detected locally.

`finish_input()` drains admitted PCM and pending flushes, sends plain text `close`, and accepts only normal `done` as graceful completion. Unconfirmed tails never become Final. Its original deadline survives finished-input Drop, pings, output backpressure, and acknowledgement activity. A later error preserves already-admitted confirmed Finals before the error, while discarding obsolete queued partials; terminal failure never depends on free output capacity. Explicit cancellation, output/control Drop, legacy `close`, and runtime stop retain bounded abort semantics and do not promise graceful final transcripts. Reconnect is fresh, without PCM replay.

## Cartesia TTS

The full Cartesia sample selects both providers. `full-duplex-demo/configs/voice-runtime.cartesia-tts.yaml` remains available for typed input plus Cartesia TTS diagnosis. The keyless default sample remains inject ASR plus mock TTS.

Set all provider-specific common fields explicitly: `tts.provider: cartesia`, endpoint `wss://api.cartesia.ai/tts/websocket`, model `sonic-3.6`, `api_key_env: CARTESIA_API_KEY`, `voice_env: CARTESIA_VOICE_ID`, language `ko`, and rate `24000`. Changing just the provider name keeps the Qwen defaults and is not a supported migration. The factory accepts `sonic-3.6`, `sonic-3.5`, `sonic-3`, and `sonic-latest`; model aliases and account/voice availability still require paid validation. Session language preserves the full selector, including region/script subtags such as `en-GB`, `fr-CA`, and `zh-Hant-TW`. Only historical word aliases `Korean → ko` and `English → en` are rewritten. This changes earlier `en-GB → en` and `ko-KR → ko` wire behavior and can affect pronunciation or provider compatibility. Code admission does not establish model/voice support or adapt agent prompts and text policies.

| Key | Default | Validation and use |
|---|---|---|
| `tts.cartesia.api_version` | `2026-08-14` | Currently the only supported schema version; sent as `Cartesia-Version`, not inferred from a test date |
| `tts.cartesia.max_buffer_delay_ms` | 0 | Integer 0–5000; sent explicitly and unchanged on every continuation and finish |

Cartesia settings and credentials are validated by its factory, not by generic YAML loading or unused provider strings on the host-injected path. Missing nested settings preserve old YAML compatibility. Endpoint queries and embedded credentials are rejected. Production endpoints must use WSS; local test servers may use WS on loopback. Voice is required at session open. Requested rates must be 8000, 16000, 22050, 24000, 44100, or 48000 Hz; output is raw mono `pcm_s16le`. `tts.chunk_ms` does not control Cartesia's server chunks: the adapter bounds and normalizes received chunks instead.

A response owns one socket and a unique context, independent text writing and audio reading, a 64-command budget including writer-held text, a 16-item normalized audio queue, and a separate terminal slot. Finish reserves no command-queue capacity: the writer drains accepted fragments before sending empty text with `continue:false`. Accepted text counts toward the cumulative byte budget; a fragment whose JSON representation cannot fit the wire budget is also rejected. WebSocket frames/messages are limited to 262144 bytes, decoded raw audio to 65536 bytes, and normalized audio to 8192 bytes. Socket writes are bounded at two seconds and the remaining response deadline. The absolute deadline starts at first successful nonempty admission, never resets on append, and applies even when output is unpolled or full. Successful finish followed by input Drop permits output drain; other cancellation/Drop paths close ownership. Late continuations are conservatively rejected after one second without received audio once audio has started; context expiry never creates a replacement context. This does not promise arbitrary host think-time.

The dedicated sample disables premade warmup and uses the reservation task spec. It requires `OPENAI_API_KEY` as well as Cartesia credentials; task-spec probe failure is strict, not a mock fallback. Enabling premade synthesis later can incur costs before browser input.

## Language scope

The library does not implicitly install a language grammar, slot heuristic, or spoken phrase. The reservation samples configure Korean automatic resources (backchannel, stop acknowledgement, failure, progress, and keyless mock reply) and enable semantic LLM interpretation without Korean rule collections. `asr.locale` and `tts.language` have no default; the built-in network providers refuse to start without them, and mock/inject providers do not need them. The task agent receives `tts.language` as `speech_language` in its turn brief; the demo task spec tells the model to reply in it, because the voice synthesizes one configured language per session. With `tts.language` unset the brief has no `speech_language` line and the demo spec replies in the user's language. The runtime does not detect or switch languages. If semantic inference is unavailable, neutral timing and conservative overlap control remain active. Multilingual end-to-end quality remains unverified.

Cartesia selectors are trimmed, limited to 63 ASCII bytes, and accept a 2–8 letter primary subtag followed by nonempty 1–8 alphanumeric subtags separated by `-`. `_` is accepted as a separator alias. Empty values, `auto`, malformed separators, control characters, and query delimiters are rejected. ASR lowercases the primary code; TTS retains the selector's case and region/script content. Factory and session validation use the same resolver. Together retains its encoded full locale and Qwen its provider-native language string. No discovery, language detection, translation, or silent provider/model fallback is performed.

## Text policy and spoken resources

Library text-rule collections are empty by default. Configure `turn_control.hard_stop_phrases`, `backchannel_words`, `complete_endings`, and `incomplete_endings` explicitly; they are literals, not regexes. Hard-stop literals use case-insensitive substring matching without stripping punctuation. Compatibility backchannels use case folding and collapsed whitespace while ignoring punctuation, with no six-character cutoff. Endings use folded suffix matching with trailing sentence punctuation removed; punctuation-only endings match the raw tail. Incomplete endings take precedence over complete endings. Explicit hard stop takes precedence over backchannel and correction; unknown overlap still aborts conservatively.

| Key | Library default | Contract |
|---|---|---|
| `turn_control.text_rules.correction` | `[]` | Regex list producing advisory Correction cues |
| `turn_control.text_rules.negation` | `[]` | Regex list producing advisory Negation cues |
| `turn_control.text_rules.intents` | `{}` | Intent name to regex list; first matching name in sorted order wins |
| `turn_control.text_rules.entities` | `{}` | Slot name to regex; retains the first matched text verbatim, without domain normalization |
| `turn_control.slot_fallback` | `neutral` | `neutral` yields no slot updates; `korean_example` explicitly enables the reservation heuristic |
| `speech.backchannel_text` | `""` | Trusted automatic backchannel text; empty disables the act, not floor control |
| `speech.hard_stop_ack_text` | `""` | Empty suppresses only the spoken ACK; hard-stop Abort and epoch/queue recovery remain active |
| `speech.failure_text` | `""` | Optional trusted failure/repeat prompt, spoken when a task reply is rejected, fails, or is lost to a TTS failure; empty does not skip failure resolution |
| `speech.outcomes` | `{}` | Outcome rules a strict reply may name, described below |
| `speech.mock_reply` | absent | Reply text of the keyless mock task agent; 1–4096 bytes |

At most 256 text rules are accepted, each 1–1024 bytes; regexes are checked against a 1 MiB compilation limit and syntax minimum-length analysis rejects zero-width paths, including assertion-only word boundaries that do not match the empty string. Rules compile at construction, not on the audio/control tick. Spoken automatic resources are at most 4096 bytes. Progress templates are limited to 128, require nonempty text of at most 4096 bytes, and a positive, overflow-safe `expire_after_ms`. Their existing latency setting controls eligibility, not a delayed scheduling guarantee.

When `agents.interaction.enabled` is true, a successfully configured semantic provider receives bounded current transcript, recent committed user turns, and confirmed heard assistant text. The runtime permits one request in flight and follows with the latest changed snapshot. A missing, failed, or timed-out provider yields neutral timing behavior rather than silently installing the Korean example heuristic. The first failure of a streak is logged as a warning (`semantic interpretation unavailable`) and recovery as info; timeouts are logged once and counted in `voice_semantic_timeout_total`. The shipped specs use `gpt-5.4-mini`. In a live check, hosted small models (`gpt-5.4-mini`, `gpt-5.4-nano`, `gpt-4.1-mini`, `gpt-4o-mini`) took about 1.1–2 s per assessment, so the library default of 900 ms timed out on every turn and turns used neutral timing. The samples set `agents.interaction.timeout_ms: 1500`; an overlap without a semantic result now waits up to 3 s before the conservative abort. Choose the model and this timeout together. Explicit host `TextAnalyzer` or legacy decision-brain overrides select the compatibility path; an explicit host slot-extractor factory overrides model slot updates. In semantic mode the configured `slot_fallback` is not used, including when the model is unavailable. With interaction disabled, configured compatibility rules and `slot_fallback` retain their existing purpose.

The interaction spec now describes semantic interpretation and must return `state`, optional `complete`, `stop`, `negated`, `intent`, `updates`, and `confidence`; the earlier `user_state`/`action` JSON is not accepted by this semantic path. The configured adapter calls the spec's default LLM provider directly with its plain system prompt and current snapshot, without retaining prior partials in agent memory. Agent state machines, tools, templated system prompts, and model routing in an old controller spec are not executed by this adapter. Update custom specs or explicitly supply a legacy `InteractionBrain` when retaining the old contract.

While the assistant is speaking, a `backchannel` or `noise_or_echo` label discards the overlapping speech, so a wrong label silently drops the user's words. The shipped interaction spec therefore defines each overlap label and sets `temperature: 0` on its model. With the ai-agents default of 0.7, a live check labeled the same overlapping question as a backchannel in 2 of 3 runs before an interruption label won; with the definitions and temperature 0 it was an interruption in 5 of 5. Set `RUST_LOG=voice_live=debug` to log each label (never the transcript) and every discarded overlap.

A `stop` label is matched to the user's words (the same candidate text, utterance, and connection generation), not to the assistant's playback state, so it still applies when the reply has moved to its next clause or ducked while the model was judging. Once a stop is applied, the stop words are consumed: they do not become a task turn, and later labels for the same words cannot cut off the stop acknowledgement. Words that combine a stop with a new request (for example "stop, and book the other one") are consumed too; the user has to repeat the request.

Host composition uses `VoiceRuntime::build_with_policies(config, optional_providers, RuntimePolicies)` or the direct builder. `RuntimePolicies::interaction_brain` and `slot_extractor` are per-session factories; existing exhaustive struct literals must add the new field or use `..Default::default()`. `InteractionSnapshot` now includes `heard_assistant` and `recent_user_turns`, and `SemanticFrame` includes `negated`; exhaustive struct literals require migration. `VoiceSessionBuilder::try_build` reports invalid configuration; the compatible `build` convenience method panics on invalid configuration.

## Strict replies and outcomes

A strict Final is either plain reply text or one JSON object:

```json
{"text": "<spoken reply>", "outcome": null, "question": null}
```

- `text` (required, nonempty) is spoken as the model wrote it.
- `outcome` (optional) names an entry of `speech.outcomes`. Leave it out or `null` for general replies.
- `question` (optional) is a turn-taking hint for the last clause: `yes_no`, `choice`, `free_text`, or `number`. It is not evidence.

A Final that does not start with `{` is read as `text` with no outcome. A Final that starts with `{` must match the object above; unknown fields are rejected. The original Final is preserved as the framework memory lookup identity.

`speech.outcomes.<name>` is an outcome rule:

| Field | Contract |
|---|---|
| `operation` | Canonical tool operation whose executed records confirm the outcome |
| `class` | `factual` or `transactional`; the claim class of the reply when the outcome is confirmed |
| `receipt_fields` | Nonempty mapping; each field of the tool's JSON receipt must equal the configured value |
| `bindings` | Nonempty list; each bound receipt field must equal the executed argument, or its `argument_defaults` value when the argument was omitted |
| `argument_defaults` | Optional mapping of backend defaults for omitted arguments |

Records are selected by operation from the executor's observations in the same turn. The model never supplies a call ID or arguments. Every record of the operation in that turn must be usable (executed, successful, not timed out, cancelled, or truncated) and pass the receipt and binding checks; one failed attempt rejects the outcome. The shipped `reserved` rule binds the restaurant, date, time, party size, customer name, and window-seat request; the omitted window-seat argument defaults to `false`, matching the backend. Its receipt status is `committed`; `availability_checked` uses `searched`.

Independently of the reply, if a tool not declared `pure_read` in `tools.effects` was started and not completed, completed unsuccessfully, or left an unusable record in the turn, the reply is not spoken. A tool declared with any other effect always counts; an undeclared tool counts only if it actually ran in the turn (a start, an executed record, or a proof). A call to a tool that never ran, such as a name the model made up, cannot have written and does not block the reply. A rejected reply is recorded as undelivered in memory, and the user hears `speech.failure_text` when it is configured.

At most 128 outcome rules are accepted, names have at most 128 bytes, operation IDs at most 256 bytes, and receipt/default mappings and bindings at most 32 entries each. Configured receipt/default values have at most 1024 serialized bytes. Finals have at most 16384 bytes. A custom `SpeechRenderer` is trusted application code responsible for its own domain validation, not an untrusted model classifier.

What this does not do: a reply that names no outcome is spoken without any check of its content, so the model can still say in words that something happened when no tool ran. It is blocked only when a write failed in the same turn. Arbitrary natural-language truth is not proven.

Migration from the earlier strict format on this branch: `speech.responses` and `speech.mock_response` are replaced by `speech.outcomes` and `speech.mock_reply`. `kind: prompt` resources are gone; the model writes those replies. `kind: tool_outcome` entries become outcome rules without `kind` and `text`. `{"response": …, "call_id": …, "arguments": …}` proposals are rejected; task specs must return the format above. Unknown keys under `speech` are rejected at load, so the old keys fail with an error.

Minimal YAML and direct builders do not select Korean grammar, canned replies, or a language. `speech.mode` and its `legacy` mode are removed: plain-text Finals are already spoken as written, and legacy's only remaining difference was a success-phrase detector that recognized Korean only. Remove `speech.mode` from configs; it is now a load error. `turn_control.text_rules.questions` is removed for the same reason (its only caller was legacy mode); a reply's `question` field supplies the hint. Configs that relied on the former `ko-KR`/`Korean` defaults with a network provider must set `asr.locale` and `tts.language`.

`CognitiveEvent`/`VoiceEvent` add `InvocationProof`, and `EvidenceKind`/`SessionEvidence` add exact authorized-speech bindings. Exhaustive Rust matches and struct literals may require migration. Browser messages are unchanged. New automatic phrases and domain policies need actual callers, configuration, and tests; adding a locale string alone does not supply them.

## Duplex safety limits

| Key | Default | Enforcement |
|---|---:|---|
| `asr.open_timeout_ms` | 10000 | One connect/auth/readiness attempt; idle transcript silence is not a failure |
| `asr.write_timeout_ms` | 2000 | Audio and commit socket writes |
| `asr.input_buffer_ms` | 1000 | Pending mono PCM16 sample budget, `input_sample_rate_hz * input_buffer_ms / 1000`; commands are separately bounded at 64 |
| `tts.open_timeout_ms` | 10000 | Text-session open and built-in Qwen setup; a buffered bridge does not start its provider until finish |
| `tts.language` | none | Normal and premade synthesis requests, separately from `asr.locale`; required by the built-in network TTS providers |
| `tts.request_timeout_ms` | 30000 | One response; buffered deadline starts at finish admission, native providers start at the first accepted nonempty text and must not extend it on append |
| `tts.max_input_bytes` | 65536 | Accumulated UTF-8 text bytes per response |
| `speech.playback_stop_ack_timeout_ms` | 1000 | After Abort, await interrupted ACK before conservative settlement; valid range 1–5000 |
| `speech.playback_ack_timeout_ms` | 15000 | Playback inactivity after first nonempty audio; provider chunks do not refresh this timer; zero disables this existing watchdog only |

New safety limits must be positive. The sample budget is checked for overflow and must fit a positive `u32`. Buffered input waiting for the host to finish is not synthesis activity and does not trigger a request timeout. Provider owners enforce deadlines independently of output polling. The built-in buffered bridge retains one terminal separately from its normalized 16-item audio queue, so timeout cleanup does not depend on queue space.

PCM output is mono s16le. A decoded raw provider chunk is limited to 65536 bytes and normalized into at most 8192-byte chunks with per-response sequences; zero rates, odd byte lengths, rate changes, and duplicate/reordered sequences are rejected. The preferred rate is advisory: the actual emitted rate is sent to the browser and used by the ledger. Premade clips are limited to 1 MiB each and cached only after valid nonempty audio and a successful terminal.

## Injected provider settings and migration

`VoiceRuntime::build_with_providers(config, SpeechProviders { asr, tts })` shares the built-in assembly but skips provider-name construction. Endpoint, model, and API-key environment settings are host-owned on this path. Common locale/manual-commit, language/voice, timing, premade, and safety budgets still apply to sessions. Factories must report their capabilities honestly; a serial-only ASR is rejected rather than disguised as duplex, and an `Incremental` TTS without text-stream support is rejected without opening a network connection. Legacy buffered TTS factories are adapted exactly once.

Task-spec probe failures are strict for injected providers, even if the config still says `mock`. For a custom keyless setup, omit `agents.task.spec`; the existing built-in keyless fallback remains unchanged. New config fields require updates to exhaustive Rust struct literals, although YAML omission remains compatible through serde defaults. Existing `TtsRequest`, `AsrSessionConfig`, `PlaybackSink::command`, and legacy provider trait methods remain usable. Internal voice events add reset/provenance/source information. `AsrEvent::InjectedFinal` identifies standalone duplex decorator injection independently of provider IDs; exhaustive event consumers need to handle that additive variant. These are not browser wire changes.

Together now uses the documented realtime endpoint and JSON/base64 messages rather than the previous unverified binary/session-create dialect. Explicit old endpoint overrides are not rewritten: update them to `wss://api.together.ai/v1/realtime`. The adapter waits for `session.created`, supports documented manual commit, and rejects rates other than 16 kHz. See the [official protocol](https://docs.together.ai/reference/audio-transcriptions-realtime); live model/locale compatibility still needs paid contract validation.

## Validation rules

- `soft_silence_ms` must be less than `hard_silence_ms`.
- Text rules are bounded and validated before session construction; invalid regexes and patterns matching empty input fail rather than silently disabling analysis. Hard-stop vocabulary is application-owned; the Korean sample omits "아니" without a core language-specific prohibition.
- Enabling `agents.speculative.enabled` requires a dedicated `spec`, a nonempty
  `allowed_tools` list, and `allow_pure_read: true`; every allowlisted tool must
  be `pure_read` in `tools.effects`.

## Audio sample rates

`audio.input_sample_rate_hz` is the rate **sent to the recognizer**. The client
reports its measured rate in `session_start`, and the server resamples between
them.

- Integer ratio (48000 → 16000): anti-alias decimation
- Non-integer ratio (44100 → 16000): low-pass filtering followed by linear interpolation
- Unsupported conversion: raise `ProviderError` and discard the audio (do not
  send it to the provider at the wrong rate)

## Framework integration

With `framework` enabled and `agents.task.spec` present, runtime startup probes the spec and credentials; actual `RuntimeAgent` construction through `AgentBuilder` occurs separately for each session. Built-in development setups (`mock`/`inject` ASR plus `mock` TTS) use the explicit `speech.mock_reply` text on probe failure. Without it the mock reply is empty, which is rejected like any other empty reply (the user hears `speech.failure_text` if configured). Live built-in providers and injected providers fail strictly. An injected task spec without the `framework` feature is rejected rather than silently selecting the mock agent; omit the spec for an intentionally keyless injected setup.

A new brain is created **for each session**. `RuntimeAgent` holds conversation
memory, so sharing one would mix transcripts between clients. Only the tool
backend (`ReservationStore`) is shared.
Interaction, Speculative, and Deep each create a **separate RuntimeAgent
instance** because root turns on one runtime are serialized. Enabling
Speculative requires a dedicated `spec`, a nonempty `allowed_tools` list, and
`allow_pure_read: true`; every allowlisted tool must be declared `pure_read` in
`tools.effects`.

## Task agent YAML contract

voice-live requires two things from the task agent.

**1. Declare the `context.voice` key and render it in the system prompt.**

```yaml
context:
  voice:
    type: runtime
    required: true      # ai-agents checks this at the start of every turn
    # Do not set default: it would always pass validation and could override the value set by voice-live

system_prompt: |
  ...
  Runtime state for this turn:
  {{ context.voice.brief }}
```

`brief` contains `transcript_revision`, `idempotency_key`, `speech_language` (the configured `tts.language`; omitted when unset), `semantic_frame`, and any speculative/deep results. You can also use `{{ context.voice.idempotency_key }}` separately.

Who guarantees what:

| Guarantee | Owner |
|---|---|
| Was the key ever set? | ai-agents — `required: true` checks at the start of every turn and fails the turn **before calling the LLM** if the key is absent (the user message is not recorded either). |
| Is the value **fresh** on each turn? | voice-live — the check only tests presence, so an old value from a previous turn would pass. The adapter supplies a new value before each root turn. |
| Does the prompt actually reference it, and is there no `default`? | Demo test `task_agent_declares_and_renders_the_voice_context` — without a reference the value does not appear in the prompt, and a `default` always passes the check. |

**A spec requiring `context.voice` is only for the task agent.** Using the same
spec for an agent that calls `chat()` without voice context (such as the deep
worker) fails every turn with `Required context 'voice' not provided`. The deep
worker therefore uses a dedicated spec (`agents/deep-worker.yaml`).

**2. voice-live owns the `memory:` block.**

Injecting host memory via `AgentBuilder::memory` causes the YAML `memory:` block
to be ignored entirely. voice-live reads only its `max_messages` to create an
`InMemoryStore`; if `type` is not `in-memory`, it warns and substitutes that
store (compacting is not supported).

The speculative agent YAML is exempt from this contract: speculative replies
are not heard by the user, so the framework manages its memory.
