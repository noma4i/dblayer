use statig::prelude::*;

use super::{Boundary, Snapshot};

/// What the lifecycle machine is told. A transition starts with `Begin`, ends with `Finish`, and the steps between are applied under the permit that owns it.
pub(super) enum Event {
    Begin(Boundary),
    Generation,
    Ended,
    Started(Option<String>),
    Cleared(Option<Vec<String>>),
    Finish,
    Close,
}

pub(super) struct Phases {
    pub snapshot: Snapshot,
}

#[state_machine(initial = "State::running()")]
impl Phases {
    #[state]
    fn running(&mut self, event: &Event) -> Outcome<State> {
        match event {
            Event::Begin(boundary) => {
                self.snapshot.apply(event);
                let _ = boundary;
                Transition(State::transition())
            }
            Event::Started(_) | Event::Cleared(_) => {
                self.snapshot.apply(event);
                Handled
            }
            Event::Close => {
                self.snapshot.apply(event);
                Transition(State::closed())
            }
            _ => Handled,
        }
    }

    #[state]
    fn transition(&mut self, event: &Event) -> Outcome<State> {
        match event {
            Event::Generation | Event::Ended | Event::Started(_) | Event::Cleared(_) => {
                self.snapshot.apply(event);
                Handled
            }
            Event::Finish => {
                self.snapshot.apply(event);
                Transition(State::running())
            }
            Event::Close => {
                self.snapshot.apply(event);
                Transition(State::closed())
            }
            Event::Begin(_) => Handled,
        }
    }

    #[state]
    fn closed() -> Outcome<State> { Handled }
}

pub(super) fn new() -> InitializedStateMachine<Phases> {
    Phases { snapshot: Snapshot::default() }.uninitialized_state_machine().init()
}
