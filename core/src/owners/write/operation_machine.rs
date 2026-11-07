use statig::prelude::*;

use crate::owners::write::operation_status::OperationStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OperationFacts {
    pub status: OperationStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OperationEvent {
    Restore,
    Attempt,
    Fail(OperationStatus),
    Retry,
    Close(OperationStatus),
}

struct Flow {
    facts: OperationFacts,
    refused: bool,
}

struct OperationFlow;

#[state_machine(initial = "State::idle()")]
impl OperationFlow {
    #[superstate]
    fn open(context: &mut Flow, event: &OperationEvent) -> Outcome<State> {
        match event {
            OperationEvent::Restore => Transition(match context.facts.status {
                OperationStatus::Pending => State::idle(),
                OperationStatus::Sending => State::sending(),
                OperationStatus::Failed => State::failed(),
                OperationStatus::Committed => State::committed(),
                OperationStatus::RolledBack => State::rolled_back(),
            }),
            OperationEvent::Close(OperationStatus::RolledBack) => Transition(State::rolled_back()),
            _ => {
                context.refused = true;
                Handled
            }
        }
    }

    #[superstate(superstate = "open")]
    fn live(event: &OperationEvent) -> Outcome<State> {
        match event {
            OperationEvent::Fail(OperationStatus::Failed) => Transition(State::failed()),
            OperationEvent::Close(OperationStatus::Committed) => Transition(State::committed()),
            _ => Super,
        }
    }

    #[state(superstate = "live")]
    fn idle(event: &OperationEvent) -> Outcome<State> {
        match event {
            OperationEvent::Attempt => Transition(State::sending()),
            _ => Super,
        }
    }

    #[state(superstate = "live")]
    fn sending(event: &OperationEvent) -> Outcome<State> {
        match event {
            OperationEvent::Attempt => Transition(State::sending()),
            _ => Super,
        }
    }

    #[state(superstate = "open")]
    fn failed(event: &OperationEvent) -> Outcome<State> {
        match event {
            OperationEvent::Retry => Transition(State::idle()),
            _ => Super,
        }
    }

    #[state]
    fn committed(context: &mut Flow) -> Outcome<State> {
        context.refused = true;
        Handled
    }

    #[state]
    fn rolled_back(context: &mut Flow) -> Outcome<State> {
        context.refused = true;
        Handled
    }
}

fn status_of(state: &State) -> OperationStatus {
    match state {
        State::Idle {} => OperationStatus::Pending,
        State::Sending {} => OperationStatus::Sending,
        State::Failed {} => OperationStatus::Failed,
        State::Committed {} => OperationStatus::Committed,
        State::RolledBack {} => OperationStatus::RolledBack,
    }
}

impl OperationFacts {
    /// The facts after the event, or `None` when the event does not apply to these facts.
    pub(crate) fn next(self, event: OperationEvent) -> Option<OperationFacts> {
        let mut flow = Flow { facts: self, refused: false };
        let mut machine = OperationFlow.uninitialized_state_machine().init_with_context(&mut flow);
        machine.handle_with_context(&OperationEvent::Restore, &mut flow);
        machine.handle_with_context(&event, &mut flow);
        if flow.refused {
            return None;
        }
        Some(OperationFacts { status: status_of(machine.state()) })
    }
}

const ALL: [OperationStatus; 5] = [OperationStatus::Pending, OperationStatus::Sending, OperationStatus::Failed, OperationStatus::Committed, OperationStatus::RolledBack];

impl OperationEvent {
    pub(crate) fn guard_sql(self) -> Option<String> {
        let accepted: Vec<String> = ALL.into_iter().filter(|status| OperationFacts { status: *status }.next(self).is_some()).map(|status| format!("'{}'", status.as_sql())).collect();
        (!accepted.is_empty()).then(|| format!("(status IN ({}))", accepted.join(", ")))
    }
}
