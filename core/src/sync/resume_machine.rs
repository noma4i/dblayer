use statig::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Cause {
    Event,
    Explicit,
}

pub(crate) enum ResumeEvent {
    Request(Cause),
    Finished,
}

struct Phases;

#[state_machine(initial = "State::idle()")]
impl Phases {
    #[state]
    fn idle(event: &ResumeEvent) -> Outcome<State> {
        match event {
            ResumeEvent::Request(cause) => Transition(State::running(*cause, None)),
            ResumeEvent::Finished => Handled,
        }
    }

    #[state]
    fn running(cause: &Cause, again: &Option<Cause>, event: &ResumeEvent) -> Outcome<State> {
        match event {
            ResumeEvent::Request(requested) => Transition(State::running(*cause, Some(again.map_or(*requested, |queued| queued.max(*requested))))),
            ResumeEvent::Finished => match again {
                Some(next) => Transition(State::running(*next, None)),
                None => Transition(State::idle()),
            },
        }
    }
}

pub(crate) struct ResumeMachine {
    machine: InitializedStateMachine<Phases>,
}

impl ResumeMachine {
    pub(crate) fn new() -> Self {
        Self { machine: Phases.uninitialized_state_machine().init() }
    }

    pub(crate) fn handle(&mut self, event: &ResumeEvent) -> Option<Cause> {
        let was_idle = matches!(self.machine.state(), State::Idle {});
        self.machine.handle(event);
        match (self.machine.state(), event) {
            (State::Running { cause, .. }, ResumeEvent::Request(_)) if was_idle => Some(*cause),
            (State::Running { cause, .. }, ResumeEvent::Finished) => Some(*cause),
            _ => None,
        }
    }

    pub(crate) fn is_idle(&self) -> bool {
        matches!(self.machine.state(), State::Idle {})
    }
}
