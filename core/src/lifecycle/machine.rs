use statig::prelude::*;

use super::{Change, Snapshot};

pub(super) struct Phases {
    pub snapshot: Snapshot,
}

#[state_machine(initial = "State::open()")]
impl Phases {
    #[state]
    fn open(&mut self, event: &Change) -> Outcome<State> {
        self.snapshot.apply(event);
        match event {
            Change::BeginReset => Transition(State::resetting()),
            Change::Close => Transition(State::closed()),
            _ => Handled,
        }
    }

    #[state]
    fn resetting(&mut self, event: &Change) -> Outcome<State> {
        self.snapshot.apply(event);
        match event {
            Change::Reset(_) => Transition(State::open()),
            Change::Close => Transition(State::closed()),
            _ => Handled,
        }
    }

    #[state]
    fn closed() -> Outcome<State> { Handled }
}

pub(super) fn new() -> InitializedStateMachine<Phases> {
    Phases { snapshot: Snapshot::default() }.uninitialized_state_machine().init()
}
