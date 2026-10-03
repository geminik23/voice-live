use std::path::PathBuf;

use serde_json::Value;
use tokio::sync::mpsc;

use crate::events::VoiceEvent;

/// JSONL event logger per the implementation doc schema. The supervisor
/// forwards every event; this writer serializes an operational payload and
/// appends one line per event.
pub struct EventLogWriter {
    pub path: PathBuf,
}

#[derive(serde::Serialize)]
struct LogLine {
    schema_version: u32,
    session_id: String,
    sequence: u64,
    monotonic_us: u64,
    wall_clock: String,
    event: String,
    payload: Value,
}

impl EventLogWriter {
    pub fn spawn(
        mut event_rx: mpsc::Receiver<VoiceEvent>,
        base_dir: PathBuf,
        session_id: crate::ids::SessionId,
        include_sensitive_payloads: bool,
    ) -> tokio::task::JoinHandle<()> {
        let path = base_dir
            .join(chrono::Local::now().format("%Y-%m-%d").to_string())
            .join(session_id.to_string());
        let path = path.join("events.jsonl");

        tokio::spawn(async move {
            if let Some(parent) = path.parent()
                && std::fs::create_dir_all(parent).is_err()
            {
                return;
            }

            let mut sequence: u64 = 0;
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path);

            let Ok(mut file) = file else {
                tracing::warn!("event log disabled: cannot open {}", path.display());
                return;
            };

            use std::io::Write;

            while let Some(event) = event_rx.recv().await {
                sequence += 1;

                let (monotonic_us, wall_clock) = event_clock(&event);

                let line = LogLine {
                    schema_version: 1,
                    session_id: session_id.to_string(),
                    sequence,
                    monotonic_us,
                    wall_clock,
                    event: event.kind().to_string(),
                    payload: event_payload(&event, include_sensitive_payloads),
                };

                if let Ok(serialized) = serde_json::to_string(&line)
                    && writeln!(file, "{serialized}").is_err()
                {
                    break;
                }
            }

            let _ = file.flush();
        })
    }
}

fn event_clock(event: &VoiceEvent) -> (u64, String) {
    let meta = match event {
        VoiceEvent::LocalSpeechStarted { meta, .. } => meta,
        VoiceEvent::LocalSpeechEnded { meta, .. } => meta,
        VoiceEvent::AsrPartial { meta, .. } => meta,
        VoiceEvent::StableTranscriptChanged { meta, .. } => meta,
        VoiceEvent::AsrUtteranceFinal { meta, .. } => meta,
        VoiceEvent::AsrStreamReset { meta, .. } => meta,
        VoiceEvent::SemanticCue { meta, .. } => meta,
        VoiceEvent::SemanticFrameUpdated { meta, .. } => meta,
        VoiceEvent::InteractionDecision { meta, .. } => meta,
        VoiceEvent::UserTurnCommitted { meta, .. } => meta,
        VoiceEvent::AgentStarted { meta, .. } => meta,
        VoiceEvent::AgentChunk { meta, .. } => meta,
        VoiceEvent::AgentFinal { meta, .. } => meta,
        VoiceEvent::AgentFailed { meta, .. } => meta,
        VoiceEvent::ToolStarted { meta, .. } => meta,
        VoiceEvent::ToolCompleted { meta, .. } => meta,
        VoiceEvent::ToolExecuted { meta, .. } => meta,
        VoiceEvent::TtsAudioChunk { meta, .. } => meta,
        VoiceEvent::TtsAudioDone { meta, .. } => meta,
        VoiceEvent::TtsFailed { meta, .. } => meta,
        VoiceEvent::PlaybackProgress { meta, .. } => meta,
        VoiceEvent::PlaybackCompleted { meta, .. } => meta,
        VoiceEvent::PlaybackInterrupted { meta, .. } => meta,
        VoiceEvent::DeepWorkRequested { meta, .. } => meta,
        VoiceEvent::DeepWorkResult { meta, .. } => meta,
        VoiceEvent::ProviderError { meta, .. } => meta,
        VoiceEvent::SessionClosed { meta, .. } => meta,
    };

    (
        meta.monotonic_us,
        meta.wall_clock
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
    )
}

fn event_payload(event: &VoiceEvent, include_sensitive: bool) -> Value {
    match event {
        VoiceEvent::AsrPartial {
            revision,
            hypothesis,
            ..
        } => serde_json::json!({
            "revision": revision.0,
            "hypothesis": sensitive_text(hypothesis, include_sensitive),
        }),
        VoiceEvent::StableTranscriptChanged { stable_prefix, .. } => {
            serde_json::json!({ "stable_prefix": sensitive_text(stable_prefix, include_sensitive) })
        }
        VoiceEvent::AsrUtteranceFinal {
            transcript,
            source,
            utterance_id,
            ..
        } => {
            serde_json::json!({ "source": source, "utterance_id": utterance_id, "transcript": sensitive_text(transcript, include_sensitive) })
        }
        VoiceEvent::AsrStreamReset { generation, .. } => {
            serde_json::json!({ "generation": generation })
        }
        VoiceEvent::UserTurnCommitted {
            transcript,
            transcript_revision,
            ..
        } => serde_json::json!({
            "transcript": sensitive_text(transcript, include_sensitive),
            "transcript_revision": transcript_revision.0,
        }),
        VoiceEvent::AgentFinal { text, .. } => serde_json::json!({
            "text": sensitive_text(text, include_sensitive)
        }),
        VoiceEvent::AgentFailed { message, .. } => serde_json::json!({ "message": message }),
        VoiceEvent::ToolStarted { tool, .. } => serde_json::json!({ "tool": tool }),
        VoiceEvent::ToolCompleted {
            tool,
            success,
            output,
            ..
        } => serde_json::json!({ "tool": tool, "success": success, "output": output }),
        VoiceEvent::ToolExecuted {
            tool,
            call_id,
            executed,
            ..
        } => serde_json::json!({ "tool": tool, "call_id": call_id, "executed": executed }),
        VoiceEvent::TtsAudioChunk {
            speech_id,
            speech_epoch,
            sequence,
            pcm,
            ..
        } => serde_json::json!({
            "speech_id": speech_id.to_string(),
            "speech_epoch": speech_epoch,
            "sequence": sequence,
            "pcm_bytes": pcm.len(),
        }),
        VoiceEvent::PlaybackCompleted { speech_id, .. } => serde_json::json!({
            "speech_id": speech_id.to_string(),
        }),
        VoiceEvent::PlaybackInterrupted {
            speech_id,
            played_samples,
            reason,
            ..
        } => serde_json::json!({
            "speech_id": speech_id.to_string(),
            "played_samples": played_samples,
            "reason": reason,
        }),
        VoiceEvent::ProviderError {
            component,
            recoverable,
            message,
            ..
        } => serde_json::json!({
            "component": component,
            "recoverable": recoverable,
            "message": message,
        }),
        VoiceEvent::SessionClosed { summary, .. } => {
            if include_sensitive {
                serde_json::to_value(summary).unwrap_or(Value::Null)
            } else {
                serde_json::json!({
                    "floor": summary.floor,
                    "task": summary.task,
                    "speech": summary.speech,
                    "transcript_revision": summary.transcript_revision,
                    "thought_epoch": summary.thought_epoch,
                    "committed_turns": summary.committed_turns,
                    "audible_clauses": summary.audible_clauses,
                    "pending_speech": summary.pending_speech,
                })
            }
        }
        _ => Value::Null,
    }
}

fn sensitive_text(text: &str, include_sensitive: bool) -> Value {
    if include_sensitive {
        Value::String(text.to_string())
    } else {
        serde_json::json!({
            "redacted": true,
            "chars": text.chars().count(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::Revision;
    use crate::meta::EventMeta;

    fn meta() -> EventMeta {
        EventMeta {
            monotonic_us: 42,
            wall_clock: chrono::Utc::now(),
        }
    }

    #[test]
    fn transcript_payload_is_redacted_by_default() {
        let event = VoiceEvent::AsrPartial {
            meta: meta(),
            utterance_id: 1,
            revision: Revision(3),
            hypothesis: "민감한 예약 내용".into(),
        };

        let redacted = event_payload(&event, false);
        let included = event_payload(&event, true);

        assert_eq!(redacted["hypothesis"]["redacted"], true);
        assert_eq!(redacted["hypothesis"]["chars"], 9);
        assert_eq!(included["hypothesis"], "민감한 예약 내용");
    }
}
