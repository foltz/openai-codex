use super::GoalAccountingInner;
use super::GoalAccountingState;
use super::GoalTurnAccounting;
use codex_protocol::config_types::ModeKind;
use codex_protocol::protocol::TokenUsage;
use std::sync::atomic::Ordering;
use std::time::Instant;

/// Preparation owns its baseline without publishing a thread-wide turn owner.
pub(crate) struct GoalTurnPreparation {
    turn_id: String,
    mode: ModeKind,
    token_usage: TokenUsage,
    prepared_at: Instant,
    descendant_token_usage: i64,
    goal_mutation_revision: u64,
    pub(crate) goal_id: Option<String>,
}

#[derive(Debug)]
pub(super) struct GoalMutation {
    goal_id: Option<String>,
    changed_at: Instant,
    descendant_token_usage: i64,
}

impl GoalTurnPreparation {
    /// Default-mode activation previously sampled these baselines after the
    /// database await. Plan mode retains its earlier preparation boundary.
    pub(crate) fn activate_goal(&mut self, goal_id: String, accounting: &GoalAccountingState) {
        self.goal_id = Some(goal_id);
        self.prepared_at = Instant::now();
        self.descendant_token_usage = accounting.descendant_token_usage.load(Ordering::Relaxed);
    }
}

impl GoalAccountingInner {
    pub(super) fn note_goal_mutation(
        &mut self,
        goal_id: Option<String>,
        descendant_token_usage: i64,
    ) {
        self.goal_mutation_revision = self.goal_mutation_revision.wrapping_add(1);
        self.latest_goal_mutation = Some(GoalMutation {
            goal_id,
            changed_at: Instant::now(),
            descendant_token_usage,
        });
    }
}

impl GoalAccountingState {
    pub(crate) fn prepare_turn(
        &self,
        turn_id: &str,
        mode: ModeKind,
        token_usage: &TokenUsage,
    ) -> GoalTurnPreparation {
        let inner = self.inner();
        GoalTurnPreparation {
            turn_id: turn_id.to_owned(),
            mode,
            token_usage: token_usage.clone(),
            prepared_at: Instant::now(),
            descendant_token_usage: self.descendant_token_usage.load(Ordering::Relaxed),
            goal_mutation_revision: inner.goal_mutation_revision,
            goal_id: None,
        }
    }

    /// Records an authoritative Goal mutation even when its value is unchanged.
    /// A cached database read from preparation must not undo this disposition.
    pub(crate) fn note_goal_mutation(&self, goal_id: Option<String>) {
        let mut inner = self.inner();
        inner.note_goal_mutation(goal_id, self.descendant_token_usage.load(Ordering::Relaxed));
    }

    /// Persisted eligibility for a newly committed turn is distinct from
    /// whether idle wall-clock accrual is currently enabled.
    pub(crate) fn note_goal_status(&self, goal: &codex_state::ThreadGoal) {
        self.note_goal_mutation(
            matches!(
                goal.status,
                codex_state::ThreadGoalStatus::Active
                    | codex_state::ThreadGoalStatus::BudgetLimited
            )
            .then(|| goal.goal_id.clone()),
        );
    }

    pub(crate) fn commit_prepared_turn(&self, turn_id: &str, prepared: &GoalTurnPreparation) {
        if prepared.turn_id != turn_id {
            return;
        }
        let mut inner = self.inner();
        let goal_mutated = inner.goal_mutation_revision != prepared.goal_mutation_revision;
        let (goal_id, activation_at, descendant_baseline) =
            if let Some(mutation) = inner.latest_goal_mutation.as_ref().filter(|_| goal_mutated) {
                (
                    mutation.goal_id.clone(),
                    mutation.changed_at,
                    mutation.descendant_token_usage,
                )
            } else {
                (
                    prepared.goal_id.clone(),
                    prepared.prepared_at,
                    prepared.descendant_token_usage,
                )
            };
        let mut turn = GoalTurnAccounting::new(
            prepared.token_usage.clone(),
            !matches!(prepared.mode, ModeKind::Plan),
        );
        inner.current_turn_id = Some(turn_id.to_owned());
        if matches!(prepared.mode, ModeKind::Plan) {
            inner.wall_clock.active_goal_id = None;
            inner.wall_clock.last_accounted_at =
                inner.wall_clock.last_accounted_at.max(prepared.prepared_at);
            inner.budget_limit_reported_goal_id = None;
            inner.execution_failure_goal_id = None;
            inner.consecutive_execution_failure_turns = 0;
            inner.automatic_goal_turn_id = None;
            inner.consecutive_empty_turns = 0;
        } else if let Some(goal_id) = goal_id {
            if inner.budget_limit_reported_goal_id.as_deref() != Some(goal_id.as_str()) {
                inner.budget_limit_reported_goal_id = None;
            }
            if inner.wall_clock.active_goal_id.as_deref() != Some(goal_id.as_str()) {
                inner.consecutive_empty_turns = 0;
                // Do not rewind progress that was accounted while preparing.
                inner.last_accounted_descendant_token_usage = inner
                    .last_accounted_descendant_token_usage
                    .max(descendant_baseline);
                inner.wall_clock.last_accounted_at =
                    inner.wall_clock.last_accounted_at.max(activation_at);
                inner.wall_clock.active_goal_id = Some(goal_id.clone());
            }
            turn.active_goal_id = Some(goal_id);
        } else if goal_mutated && inner.wall_clock.active_goal_id.is_some() {
            inner.wall_clock.clear_active_goal();
        }
        inner.turns.insert(turn_id.to_owned(), turn);
    }
}
