#![allow(dead_code)]

#[path = "../src/accounting.rs"]
mod accounting;

use accounting::BudgetLimitedGoalDisposition;
use accounting::GoalAccountingState;
use codex_extension_api::ToolCallOutcome;
use codex_extension_api::ToolName;
use codex_protocol::config_types::ModeKind;
use codex_protocol::items::AgentMessageContent;
use codex_protocol::items::AgentMessageItem;
use codex_protocol::items::EnteredReviewModeItem;
use codex_protocol::items::TurnItem;
use codex_protocol::models::MessagePhase;
use codex_protocol::protocol::ReviewTarget;
use codex_protocol::protocol::TokenUsage;
use codex_state::ThreadGoalStatus;
use pretty_assertions::assert_eq;

#[test]
fn abandoned_preparation_preserves_every_accounting_field_and_late_review_is_inert() {
    for mode in [ModeKind::Plan, ModeKind::Default] {
        let state = GoalAccountingState::default();
        let mut previous =
            state.prepare_turn("previous", ModeKind::Default, &TokenUsage::default());
        previous.goal_id = Some("goal".into());
        state.commit_prepared_turn("previous", &previous);
        state.mark_goal_continuation("previous".into());
        state.record_tool_outcome(
            "previous",
            &ToolName::plain("exec"),
            ToolCallOutcome::Failed {
                handler_executed: true,
            },
        );
        state.execution_failure_goal("previous");
        state.finish_turn("previous");
        let mut empty = state.prepare_turn("empty", ModeKind::Default, &TokenUsage::default());
        empty.goal_id = Some("goal".into());
        state.commit_prepared_turn("empty", &empty);
        state.mark_goal_continuation("empty".into());
        state.record_item(
            "empty",
            &TurnItem::AgentMessage(AgentMessageItem {
                id: "empty-final".into(),
                content: vec![AgentMessageContent::Text { text: "".into() }],
                phase: Some(MessagePhase::FinalAnswer),
                memory_citation: None,
                delivery: None,
                questions: None,
            }),
        );
        state.empty_response_goal("empty");
        state.mark_budget_limit_reported_if_new("goal");
        state.finish_turn("empty");
        // Debug includes the complete private accounting state, including the
        // wall-clock Instant, all counters, retained entries and baselines.
        let before = format!("{state:?}");
        let retained: accounting::GoalTurnPreparation =
            state.prepare_turn("refused", mode, &TokenUsage::default());
        state.record_item(
            "refused",
            &TurnItem::EnteredReviewMode(EnteredReviewModeItem {
                id: "review".into(),
                target: ReviewTarget::UncommittedChanges,
                user_facing_hint: "review".into(),
            }),
        );
        assert_eq!(before, format!("{state:?}"));
        let mut winner = state.prepare_turn("winner", ModeKind::Default, &TokenUsage::default());
        winner.goal_id = Some("goal".into());
        state.commit_prepared_turn("winner", &winner);
        let installed = format!("{state:?}");
        drop(retained);
        assert_eq!(installed, format!("{state:?}"));
        assert_eq!(Some("winner".into()), state.current_turn_id());
    }
}

#[test]
fn committed_preparation_uses_original_tokens_before_any_item() {
    let state = GoalAccountingState::default();
    let baseline = TokenUsage {
        input_tokens: 100,
        total_tokens: 100,
        ..Default::default()
    };
    let mut prepared = state.prepare_turn("turn", ModeKind::Default, &baseline);
    prepared.goal_id = Some("goal".into());
    assert_eq!(None, state.current_turn_id());
    state.commit_prepared_turn("turn", &prepared);
    state.record_token_usage(
        "turn",
        &TokenUsage {
            input_tokens: 123,
            total_tokens: 123,
            ..Default::default()
        },
    );
    assert_eq!(
        23,
        state
            .progress_snapshot("turn")
            .expect("accepted turn")
            .token_delta
    );
}

#[test]
fn newer_goal_disposition_wins_over_cached_preparation_even_when_unchanged() {
    for newer in [None, Some("cached"), Some("replacement")] {
        let state = GoalAccountingState::default();
        let mut prepared = state.prepare_turn("turn", ModeKind::Default, &TokenUsage::default());
        prepared.goal_id = Some("cached".into());
        state.note_goal_mutation(newer.map(str::to_owned));
        state.commit_prepared_turn("turn", &prepared);
        assert_eq!(
            newer.map(str::to_owned),
            state.current_active_goal_id_for_turn("turn")
        );
    }
}

#[test]
fn goal_created_during_preparation_does_not_charge_earlier_time_or_descendants() {
    let state = GoalAccountingState::default();
    let prepared = state.prepare_turn("turn", ModeKind::Default, &TokenUsage::default());
    state.record_descendant_token_usage(&TokenUsage {
        input_tokens: 12,
        total_tokens: 12,
        ..Default::default()
    });
    std::thread::sleep(std::time::Duration::from_millis(1100));
    state.note_goal_mutation(Some("new-goal".into()));
    state.record_descendant_token_usage(&TokenUsage {
        input_tokens: 7,
        total_tokens: 7,
        ..Default::default()
    });
    state.commit_prepared_turn("turn", &prepared);
    let snapshot = state
        .progress_snapshot("turn")
        .expect("newly activated goal");
    assert_eq!(7, snapshot.token_delta);
    assert_eq!(0, snapshot.time_delta_seconds);
}

#[test]
fn idle_progress_during_preparation_is_not_rewound_or_double_charged() {
    let state = GoalAccountingState::default();
    state.mark_idle_goal_active("goal");
    let mut prepared = state.prepare_turn("turn", ModeKind::Default, &TokenUsage::default());
    prepared.goal_id = Some("goal".into());
    state.record_descendant_token_usage(&TokenUsage {
        input_tokens: 10,
        total_tokens: 10,
        ..Default::default()
    });
    let idle = state.idle_progress_snapshot().expect("idle goal");
    state.mark_idle_progress_accounted_for_status(
        &idle,
        ThreadGoalStatus::Active,
        BudgetLimitedGoalDisposition::KeepActive,
    );
    state.record_descendant_token_usage(&TokenUsage {
        input_tokens: 7,
        total_tokens: 7,
        ..Default::default()
    });
    state.commit_prepared_turn("turn", &prepared);
    assert_eq!(
        7,
        state
            .progress_snapshot("turn")
            .expect("installed goal")
            .token_delta
    );
    // Idle BudgetLimited clearing stops idle accrual, but does not revoke the
    // existing eligibility of a BudgetLimited Goal for a newly installed turn.
    let other = GoalAccountingState::default();
    other.mark_idle_goal_active("goal");
    let mut prepared = other.prepare_turn("turn", ModeKind::Default, &TokenUsage::default());
    prepared.goal_id = Some("goal".into());
    other.record_descendant_token_usage(&TokenUsage {
        input_tokens: 1,
        total_tokens: 1,
        ..Default::default()
    });
    let idle = other.idle_progress_snapshot().expect("idle goal");
    other.mark_idle_progress_accounted_for_status(
        &idle,
        ThreadGoalStatus::BudgetLimited,
        BudgetLimitedGoalDisposition::ClearActive,
    );
    other.commit_prepared_turn("turn", &prepared);
    assert_eq!(
        Some("goal".into()),
        other.current_active_goal_id_for_turn("turn")
    );
}
