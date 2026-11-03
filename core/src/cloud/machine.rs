use statig::prelude::*;

use super::{CloudError, CloudState};

pub(super) enum PhaseEvent {
    Merge { rows: u64 },
    Start,
    Done,
    Wait,
    Stop(CloudError),
    GaveUp(CloudError),
}

struct Phases;

fn stopped(error: &CloudError) -> State {
    match error {
        CloudError::Offline { .. } | CloudError::Unavailable { .. } | CloudError::Throttled { .. } => State::offline(),
        CloudError::NeedsAuthorization { .. } => State::needs_authorization(),
        CloudError::QuotaExceeded { .. } => State::quota_exceeded(),
        CloudError::Rejected { detail } => State::failed(detail.clone()),
        CloudError::Interrupted { .. } => State::idle(),
    }
}

#[state_machine(initial = "State::idle()")]
impl Phases {
    #[superstate]
    fn running(event: &PhaseEvent) -> Outcome<State> {
        match event {
            PhaseEvent::Merge { rows } => Transition(State::needs_initial_merge(*rows)),
            PhaseEvent::Start => Transition(State::syncing()),
            PhaseEvent::Wait => Transition(State::offline()),
            PhaseEvent::Stop(error) => Transition(stopped(error)),
            PhaseEvent::GaveUp(CloudError::Unavailable { detail }) => Transition(State::failed(detail.clone())),
            PhaseEvent::GaveUp(error) => Transition(State::failed(error.to_string())),
            PhaseEvent::Done => Handled,
        }
    }

    #[state(superstate = "running")]
    fn idle() -> Outcome<State> {
        Super
    }

    #[state(superstate = "running")]
    fn syncing(event: &PhaseEvent) -> Outcome<State> {
        match event {
            PhaseEvent::Done => Transition(State::idle()),
            _ => Super,
        }
    }

    #[state(superstate = "running")]
    fn offline() -> Outcome<State> {
        Super
    }

    #[state(superstate = "running")]
    fn needs_authorization() -> Outcome<State> {
        Super
    }

    #[state(superstate = "running")]
    fn quota_exceeded() -> Outcome<State> {
        Super
    }

    #[state(superstate = "running")]
    fn needs_initial_merge(rows: &u64) -> Outcome<State> {
        let _ = rows;
        Super
    }

    #[state(superstate = "running")]
    fn failed(detail: &String) -> Outcome<State> {
        let _ = detail;
        Super
    }
}

pub(super) struct Phase {
    machine: InitializedStateMachine<Phases>,
}

impl Phase {
    pub(super) fn new() -> Self {
        Self { machine: Phases.uninitialized_state_machine().init() }
    }

    pub(super) fn handle(&mut self, event: &PhaseEvent) -> CloudState {
        self.machine.handle(event);
        match self.machine.state() {
            State::Idle {} => CloudState::Idle,
            State::Syncing {} => CloudState::Syncing,
            State::Offline {} => CloudState::Offline,
            State::NeedsAuthorization {} => CloudState::NeedsAuthorization,
            State::QuotaExceeded {} => CloudState::QuotaExceeded,
            State::NeedsInitialMerge { rows } => CloudState::NeedsInitialMerge { rows: *rows },
            State::Failed { detail } => CloudState::Failed { detail: detail.clone() },
        }
    }
}
