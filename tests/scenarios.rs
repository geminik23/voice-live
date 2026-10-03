//! Virtual-time scenario suite from the implementation doc: the deterministic
//! regression harness runs every documented Korean full-duplex scenario
//! offline with fake providers.

use voice_live::{Scenario, ScenarioRunner};

macro_rules! scenario_test {
    ($name:ident, $yaml:expr) => {
        #[tokio::test(start_paused = true)]
        async fn $name() {
            let scenario: Scenario = ScenarioRunner::load_yaml($yaml).expect("scenario parses");
            let report = ScenarioRunner::run(&scenario).await;

            if !report.passed {
                for failure in &report.failures {
                    eprintln!("[{}] FAIL: {failure}", scenario.name);
                }
                eprintln!(
                    "[{}] commands: {:?}",
                    scenario.name,
                    report
                        .commands
                        .iter()
                        .map(|c| c.kind.clone())
                        .collect::<Vec<_>>()
                );
                eprintln!(
                    "[{}] events: {:?}",
                    scenario.name,
                    report
                        .events
                        .iter()
                        .map(|e| e.kind.clone())
                        .collect::<Vec<_>>()
                );
            }

            assert!(
                report.passed,
                "scenario '{}' failed: {:#?}",
                scenario.name, report.failures
            );
        }
    };
}

scenario_test!(
    backchannel_ducks_then_resumes,
    r#"
name: backchannel_during_agent_speech
initial:
  assistant_speaking: true
  assistant_asked_question: false
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 80
    event:
      type: asr_partial
      revision: 1
      text: "네"
  - at_ms: 180
    event:
      type: local_speech_ended
expect:
  - type: command_within
    command: duck
    within_ms: 120
  - type: command
    command: resume
  - type: never_command
    command: abort
  - type: no_user_turn_committed
settle_ms: 800
"#
);

scenario_test!(
    correction_invalidates_date_dependent_search,
    r#"
name: correction_invalidates_date_dependent_search
initial:
  semantic_frame:
    date: "금요일"
    time: "19:00"
  active_tasks:
    - id: availability_search
      dependencies: [date, time, party_size]
    - id: user_profile_lookup
      dependencies: [user_id]
timeline:
  - at_ms: 0
    event:
      type: asr_partial
      revision: 31
      text: "아니 금요일 말고 토요일"
expect:
  - type: task_cancelled
    id: availability_search
  - type: task_not_cancelled
    id: user_profile_lookup
  - type: thought_epoch_incremented
settle_ms: 600
"#
);

scenario_test!(
    hard_interruption_aborts_and_acks,
    r#"
name: hard_interruption_aborts_and_acks
initial:
  assistant_speaking: true
  assistant_recent_text: "그러면 예약을 진행하겠습니다."
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 60
    event:
      type: asr_partial
      revision: 1
      text: "잠깐"
  - at_ms: 400
    event:
      type: asr_partial
      revision: 2
      text: "잠깐 예약하지 마"
  - at_ms: 900
    event:
      type: local_speech_ended
expect:
  - type: command
    command: duck
  - type: command
    command: abort
  - type: audible_contains
    text: "멈췄어요"
  - type: never_command
    command: resume
settle_ms: 2500
"#
);

scenario_test!(
    answer_after_agent_question_commits,
    r#"
name: answer_after_agent_question_commits
initial:
  assistant_asked_question: true
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 80
    event:
      type: asr_partial
      revision: 1
      text: "네"
  - at_ms: 300
    event:
      type: local_speech_ended
expect:
  - type: user_turn_committed
    contains: "네"
settle_ms: 1500
"#
);

scenario_test!(
    incomplete_ending_waits_and_merges,
    r#"
name: incomplete_ending_waits_and_merges
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 100
    event:
      type: asr_partial
      revision: 1
      text: "저는 이번 주말에..."
  - at_ms: 900
    event:
      type: local_speech_ended
  - at_ms: 950
    event:
      type: asr_final
      text: "저는 이번 주말에"
  - at_ms: 1600
    event:
      type: local_speech_started
  - at_ms: 1700
    event:
      type: asr_partial
      revision: 2
      text: "서울에서 갈 만한 곳을 찾아주세요"
  - at_ms: 2400
    event:
      type: local_speech_ended
expect:
  - type: user_turn_committed
    contains: "주말에"
  - type: user_turn_committed
    contains: "찾아주세요"
  - type: committed_turn_count
    exactly: 1
settle_ms: 2000
"#
);

scenario_test!(
    self_echo_is_not_a_user_turn,
    r#"
name: self_echo_is_not_a_user_turn
initial:
  assistant_speaking: true
  assistant_recent_text: "토요일 저녁 일곱 시로 확인해볼게요."
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 100
    event:
      type: asr_partial
      revision: 1
      text: "토요일 저녁 일곱 시로 확인해볼게요"
  - at_ms: 600
    event:
      type: local_speech_ended
expect:
  - type: no_user_turn_committed
  - type: command
    command: duck
  - type: never_command
    command: abort
settle_ms: 1600
"#
);

scenario_test!(
    full_pipeline_commits_and_speaks,
    r#"
name: full_pipeline_commits_and_speaks
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 100
    event:
      type: asr_final
      text: "토요일 저녁 7시로 예약해줘"
  - at_ms: 400
    event:
      type: local_speech_ended
expect:
  - type: user_turn_committed
    contains: "예약해줘"
  - type: committed_transcript_equals
    text: "토요일 저녁 7시로 예약해줘"
  - type: event
    event: playback_completed
  - type: event
    event: tts_audio_done
  - type: audible_contains
    text: "확인했습니다"
settle_ms: 3000
"#
);

scenario_test!(
    stale_final_is_never_spoken,
    r#"
name: stale_final_is_never_spoken
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 100
    event:
      type: asr_partial
      revision: 1
      text: "토요일 저녁 7시로 예약해줘"
  - at_ms: 300
    event:
      type: local_speech_ended
  - at_ms: 1400
    event:
      type: local_speech_started
  - at_ms: 1500
    event:
      type: asr_partial
      revision: 2
      text: "아니 금요일 말고 토요일"
  - at_ms: 2100
    event:
      type: local_speech_ended
agent_script:
  - after_ms: 1200
    type: final
    text: "금요일 기준으로 두 곳이 가능합니다."
expect:
  - type: audible_excludes
    text: "금요일 기준"
  - type: metric_at_least
    name: voice_task_stale_result_dropped_total
    value: 1
settle_ms: 1000
"#
);

scenario_test!(
    transactional_claim_requires_action_commit,
    r#"
name: transactional_claim_requires_action_commit
config:
  tools:
    effects:
      reserve_restaurant: transactional_write
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 100
    event:
      type: asr_partial
      revision: 1
      text: "토요일 저녁 7시로 예약해줘"
  - at_ms: 500
    event:
      type: local_speech_ended
agent_script:
  - after_ms: 100
    type: tool_started
    tool: reserve_restaurant
  - after_ms: 200
    type: tool_executed
    tool: reserve_restaurant
    executed: true
  - after_ms: 300
    type: tool_completed
    tool: reserve_restaurant
    success: true
  - after_ms: 400
    type: final
    text: "예약이 완료되었습니다."
expect:
  - type: user_turn_committed
    contains: "예약해줘"
  - type: audible_contains
    text: "예약이 완료되었습니다"
  - type: metric_at_least
    name: voice_action_commit_total
    value: 1
settle_ms: 3500
"#
);

scenario_test!(
    transactional_action_commit_is_order_independent,
    r#"
name: transactional_action_commit_is_order_independent
config:
  tools:
    effects:
      reserve_restaurant: transactional_write
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 100
    event:
      type: asr_partial
      revision: 1
      text: "토요일 저녁 7시로 예약해줘"
  - at_ms: 500
    event:
      type: local_speech_ended
agent_script:
  - after_ms: 100
    type: tool_started
    tool: reserve_restaurant
  - after_ms: 200
    type: tool_completed
    tool: reserve_restaurant
    success: true
  - after_ms: 300
    type: tool_executed
    tool: reserve_restaurant
    executed: true
  - after_ms: 400
    type: final
    text: "예약이 완료되었습니다."
expect:
  - type: audible_contains
    text: "예약이 완료되었습니다"
  - type: metric_at_least
    name: voice_action_commit_total
    value: 1
settle_ms: 3500
"#
);

scenario_test!(
    transactional_claim_blocked_without_execution_record,
    r#"
name: transactional_claim_blocked_without_execution_record
config:
  tools:
    effects:
      reserve_restaurant: transactional_write
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 100
    event:
      type: asr_partial
      revision: 1
      text: "토요일 저녁 7시로 예약해줘"
  - at_ms: 500
    event:
      type: local_speech_ended
agent_script:
  - after_ms: 100
    type: tool_started
    tool: reserve_restaurant
  - after_ms: 300
    type: tool_completed
    tool: reserve_restaurant
    success: true
  - after_ms: 400
    type: final
    text: "예약이 완료되었습니다."
expect:
  - type: audible_excludes
    text: "예약이 완료되었습니다"
  - type: metric_at_least
    name: voice_claim_rejected_total
    value: 1
settle_ms: 3500
"#
);

scenario_test!(
    progress_speech_announces_a_slow_tool,
    r#"
name: progress_speech_announces_a_slow_tool
config:
  tools:
    effects:
      search_availability: pure_read
  speech:
    progress_templates:
      search_availability:
        text: "조건에 맞는 곳을 확인하고 있어요."
        minimum_expected_latency_ms: 1200
        expire_after_ms: 4000
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 100
    event:
      type: asr_final
      text: "토요일 저녁 7시로 자리 찾아줘"
  - at_ms: 400
    event:
      type: local_speech_ended
agent_script:
  - after_ms: 100
    type: tool_started
    tool: search_availability
  - after_ms: 1600
    type: tool_completed
    tool: search_availability
    success: true
  - after_ms: 1700
    type: final
    text: "두 곳이 가능합니다."
expect:
  - type: audible_contains
    text: "확인하고 있어요"
  - type: audible_contains
    text: "두 곳이 가능합니다"
settle_ms: 6000
"#
);

scenario_test!(
    tts_failure_does_not_wedge_the_speech_queue,
    r#"
name: tts_failure_does_not_wedge_the_speech_queue
tts_chunks_per_request: 0
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 100
    event:
      type: asr_final
      text: "토요일 저녁 7시로 예약해줘"
  - at_ms: 400
    event:
      type: local_speech_ended
  - at_ms: 2600
    event:
      type: local_speech_started
  - at_ms: 2700
    event:
      type: asr_final
      text: "다른 날도 알아봐줘"
  - at_ms: 3000
    event:
      type: local_speech_ended
agent_script:
  - after_ms: 200
    type: final
    text: "확인했습니다. 두 곳이 가능합니다."
expect:
  # Each reply has two clauses, but a failed synthesis discards the rest of
  # that reply: only the first clause of each reply is ever attempted.
  - type: metric_at_least
    name: voice_tts_failed_total
    value: 2
  - type: metric_at_most
    name: voice_tts_failed_total
    value: 2
  - type: metric_at_least
    name: voice_reply_clauses_discarded_total
    value: 2
  # Nothing was delivered, so both replies resolve exactly once as
  # undelivered and nothing completes playback.
  - type: turn_resolution_count
    exactly: 2
  - type: turn_resolved
    resolution: audible
    heard_equals: "[응답이 전달되지 않음]"
  - type: never_event
    event: playback_completed
settle_ms: 6000
"#
);

scenario_test!(
    proactive_backchannel_fills_an_incomplete_pause,
    r#"
name: proactive_backchannel_fills_an_incomplete_pause
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 100
    event:
      type: asr_partial
      revision: 1
      text: "저는 이번 주말에 갈 만한 곳을 찾고 있는데"
  - at_ms: 300
    event:
      type: local_speech_ended
expect:
  # The pause sits between soft and hard silence on an incomplete connective,
  # which is exactly where a Korean listener would say "네".
  - type: metric_at_least
    name: voice_backchannel_emitted_total
    value: 1
  - type: user_turn_committed
    contains: "찾고 있는데"
settle_ms: 4000
"#
);

scenario_test!(
    deep_work_result_reaches_the_session,
    r#"
name: deep_work_result_reaches_the_session
deep_work_summary: "성수 지역 주차 가능 매장 3곳을 확인했습니다."
deep_work_delay_ms: 300
timeline:
  - at_ms: 0
    event:
      type: deep_work_requested
      objective: "주차 가능한 매장을 조사"
      dependencies: [area]
expect:
  - type: metric_at_least
    name: voice_deep_work_started_total
    value: 1
  - type: event
    event: deep_work_result
settle_ms: 2000
"#
);

scenario_test!(
    playback_ack_timeout_releases_the_speech_queue,
    r#"
name: playback_ack_timeout_releases_the_speech_queue
playback_acks: false
config:
  speech:
    playback_ack_timeout_ms: 400
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 100
    event:
      type: asr_final
      text: "토요일 저녁 7시로 예약해줘"
  - at_ms: 400
    event:
      type: local_speech_ended
agent_script:
  - after_ms: 200
    type: final
    text: "확인했습니다."
expect:
  # A client that never acknowledges must not pin SpeechState forever.
  - type: metric_at_least
    name: voice_playback_ack_timeout_total
    value: 1
settle_ms: 5000
"#
);

scenario_test!(
    hard_stop_overrides_an_uninterruptible_act,
    r#"
name: hard_stop_overrides_an_uninterruptible_act
initial:
  assistant_speaking: true
  assistant_recent_text: "예약을 진행하겠습니다."
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 60
    event:
      type: asr_partial
      revision: 1
      text: "그만"
  - at_ms: 600
    event:
      type: local_speech_ended
expect:
  # An explicit stop request must reach the client no matter what the
  # in-flight act declared about its own interruptibility.
  - type: command
    command: abort
  - type: audible_contains
    text: "멈췄어요"
  - type: metric_at_least
    name: voice_speech_aborted_total
    value: 1
settle_ms: 2500
"#
);

// ---------------------------------------------------------------------------
// Conversation memory: what each turn resolves to.
//
// These assert on the resolution reported to the agent, which is exactly what
// replaces the generated reply in framework memory.
// ---------------------------------------------------------------------------

scenario_test!(
    fully_heard_reply_is_resolved_unchanged,
    r#"
name: fully_heard_reply_is_resolved_unchanged
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 100
    event:
      type: asr_final
      text: "토요일 저녁 7시로 예약해줘"
  - at_ms: 400
    event:
      type: local_speech_ended
expect:
  - type: turn_resolved
    resolution: audible
    heard_equals_final: true
  - type: turn_resolution_count
    exactly: 1
  # Only the committed words are the user message; the environment travels
  # as turn context and never accumulates in memory.
  - type: agent_input_equals
    text: "토요일 저녁 7시로 예약해줘"
  - type: agent_context_contains
    text: "idempotency_key: voice-turn-"
  - type: agent_context_contains
    text: "semantic_frame:"
settle_ms: 3000
"#
);

scenario_test!(
    interrupted_reply_keeps_only_the_heard_prefix,
    r#"
name: interrupted_reply_keeps_only_the_heard_prefix
agent_final: "확인했습니다. 토요일 저녁 7시 기준으로 두 곳이 가능합니다."
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 100
    event:
      type: asr_final
      text: "토요일 저녁 7시로 예약해줘"
  - at_ms: 400
    event:
      type: local_speech_ended
  # Barge in during the second clause, after the first has played in full.
  # Progress is reported per finished chunk, as the browser's `onended` does.
  - at_ms: 1280
    event:
      type: local_speech_started
  - at_ms: 1300
    event:
      type: asr_partial
      revision: 5
      text: "잠깐"
expect:
  - type: command
    command: abort
  # Cancelling synthesis on barge-in is an abort, not a TTS failure.
  - type: never_event
    event: tts_failed
  # The first clause was heard in full and stays.
  - type: turn_resolved
    resolution: audible
    heard_contains: "확인했습니다."
  # The cut is recorded, and the unheard end of the second clause is gone.
  - type: turn_resolved
    resolution: audible
    heard_contains: "[중단됨"
    heard_excludes: "가능합니다"
settle_ms: 2000
"#
);

scenario_test!(
    stale_reply_is_discarded_from_memory,
    r#"
name: stale_reply_is_discarded_from_memory
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 100
    event:
      type: asr_partial
      revision: 1
      text: "토요일 저녁 7시로 예약해줘"
  - at_ms: 300
    event:
      type: local_speech_ended
  - at_ms: 1400
    event:
      type: local_speech_started
  - at_ms: 1500
    event:
      type: asr_partial
      revision: 2
      text: "아니 금요일 말고 토요일"
  - at_ms: 2100
    event:
      type: local_speech_ended
agent_script:
  - after_ms: 1200
    type: final
    text: "금요일 기준으로 두 곳이 가능합니다."
expect:
  # Never spoken, so it must not survive in memory as the assistant's reply.
  - type: turn_resolved
    resolution: discard
  - type: audible_excludes
    text: "금요일 기준"
settle_ms: 1000
"#
);

scenario_test!(
    unplayed_reply_is_recorded_as_undelivered,
    r#"
name: unplayed_reply_is_recorded_as_undelivered
tts_chunks_per_request: 0
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 100
    event:
      type: asr_final
      text: "토요일 저녁 7시로 예약해줘"
  - at_ms: 400
    event:
      type: local_speech_ended
agent_script:
  - after_ms: 200
    type: final
    text: "확인했습니다. 두 곳이 가능합니다."
expect:
  # Keeps the user/assistant pairing, but says the reply never arrived.
  - type: turn_resolved
    resolution: audible
    heard_equals: "[응답이 전달되지 않음]"
settle_ms: 4000
"#
);

scenario_test!(
    rejected_claim_is_recorded_as_undelivered,
    r#"
name: rejected_claim_is_recorded_as_undelivered
config:
  tools:
    effects:
      reserve_restaurant: transactional_write
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 100
    event:
      type: asr_partial
      revision: 1
      text: "토요일 저녁 7시로 예약해줘"
  - at_ms: 500
    event:
      type: local_speech_ended
agent_script:
  - after_ms: 100
    type: tool_started
    tool: reserve_restaurant
  - after_ms: 300
    type: tool_completed
    tool: reserve_restaurant
    success: true
  - after_ms: 400
    type: final
    text: "예약이 완료되었습니다."
expect:
  # The claim gate refused to speak an unverified reservation. Memory must not
  # keep it as though the user was told the booking succeeded.
  - type: metric_at_least
    name: voice_claim_rejected_total
    value: 1
  - type: turn_resolved
    resolution: audible
    heard_equals: "[응답이 전달되지 않음]"
    heard_excludes: "예약이 완료"
settle_ms: 3500
"#
);

scenario_test!(
    superseded_turn_is_discarded,
    r#"
name: superseded_turn_is_discarded
timeline:
  - at_ms: 0
    event:
      type: local_speech_started
  - at_ms: 100
    event:
      type: asr_final
      text: "토요일 저녁 7시로 예약해줘"
  - at_ms: 400
    event:
      type: local_speech_ended
  # The user commits another turn while the first is still reasoning.
  - at_ms: 1500
    event:
      type: local_speech_started
  - at_ms: 1600
    event:
      type: asr_final
      text: "창가 자리로 해주세요"
  - at_ms: 1800
    event:
      type: local_speech_ended
agent_script:
  - after_ms: 3000
    type: final
    text: "두 곳이 가능합니다."
expect:
  - type: metric_at_least
    name: voice_task_superseded_total
    value: 1
  - type: turn_resolved
    resolution: discard
settle_ms: 2000
"#
);
