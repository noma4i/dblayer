use statig::prelude::*;

use crate::owners::write::operation_status::OperationStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OperationFacts {
    pub status: OperationStatus,
    pub transport_started: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OperationEvent {
    Restore,
    Start,
    Postpone,
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
            OperationEvent::Restore => Transition(match (context.facts.status, context.facts.transport_started) {
                (OperationStatus::Pending, false) => State::idle(),
                (OperationStatus::Pending, true) => State::sending(),
                (OperationStatus::Failed, _) => State::failed(),
                (OperationStatus::Committed, _) => State::committed(),
                (OperationStatus::RolledBack, _) => State::rolled_back(),
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
    fn idle(context: &mut Flow, event: &OperationEvent) -> Outcome<State> {
        match event {
            OperationEvent::Start => {
                context.facts.transport_started = true;
                Transition(State::sending())
            }
            _ => Super,
        }
    }

    #[state(superstate = "live")]
    fn sending(context: &mut Flow, event: &OperationEvent) -> Outcome<State> {
        match event {
            OperationEvent::Postpone => {
                context.facts.transport_started = false;
                Transition(State::idle())
            }
            _ => Super,
        }
    }

    #[state(superstate = "open")]
    fn failed(context: &mut Flow, event: &OperationEvent) -> Outcome<State> {
        match event {
            OperationEvent::Retry => {
                context.facts.transport_started = false;
                Transition(State::idle())
            }
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
        State::Idle {} | State::Sending {} => OperationStatus::Pending,
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
        Some(OperationFacts { status: status_of(machine.state()), transport_started: flow.facts.transport_started })
    }
}

const ALL: [OperationStatus; 4] = [OperationStatus::Pending, OperationStatus::Failed, OperationStatus::Committed, OperationStatus::RolledBack];

impl OperationEvent {
    pub(crate) fn guard_sql(self) -> Option<String> {
        let mut clauses = Vec::new();
        for status in ALL {
            let accepted: Vec<bool> = [false, true].into_iter().filter(|started| OperationFacts { status, transport_started: *started }.next(self).is_some()).collect();
            match accepted.as_slice() {
                [] => {}
                [started] => clauses.push(format!("(status = '{}' AND transport_started = {})", status.as_sql(), u8::from(*started))),
                _ => clauses.push(format!("status = '{}'", status.as_sql())),
            }
        }
        (!clauses.is_empty()).then(|| format!("({})", clauses.join(" OR ")))
    }
}
