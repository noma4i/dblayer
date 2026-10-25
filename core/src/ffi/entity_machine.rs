use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use statig::prelude::*;

use crate::db::locked;

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum EntityStage {
    Opening,
    Present,
    Absent,
    Failed { message: String },
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct EntityVerdict {
    pub applied: bool,
    pub cleared: bool,
    pub stage: EntityStage,
    pub stale: bool,
    pub revision: i64,
    pub feed: u64,
}

static NEXT_FEED: AtomicU64 = AtomicU64::new(1);

enum Event {
    Seed { revision: i64 },
    Accept { revision: i64, present: bool, feed: Option<u64> },
    Fail { message: String, feed: Option<u64> },
    Reset,
    BeginFeed,
    Unopened { feed: u64 },
    Close,
}

struct Flow {
    singleton: bool,
    revision: i64,
    stale: bool,
    delivered: bool,
    active: u64,
}

#[derive(Default)]
struct Effect {
    applied: bool,
    cleared: bool,
}

impl Flow {
    fn new(singleton: bool) -> Self {
        Self { singleton, revision: i64::MIN, stale: false, delivered: false, active: 0 }
    }

    fn admits(&self, feed: Option<u64>) -> bool {
        if self.singleton {
            matches!(feed, Some(feed) if feed != 0 && feed == self.active)
        } else {
            feed.is_none()
        }
    }

    fn drop_value(&mut self, effect: &mut Effect) {
        self.revision = i64::MIN;
        self.stale = false;
        self.delivered = false;
        effect.cleared = true;
    }
}

#[state_machine(initial = "State::opening()")]
impl Flow {
    #[superstate]
    fn live(&mut self, context: &mut Effect, event: &Event) -> Outcome<State> {
        match event {
            Event::Seed { revision } if !self.delivered && *revision > self.revision => {
                self.revision = *revision;
                self.stale = true;
                context.applied = true;
                Transition(State::present())
            }
            Event::Seed { .. } => Handled,
            Event::Accept { revision, present, feed } if self.admits(*feed) && *revision >= self.revision => {
                self.revision = *revision;
                self.stale = false;
                self.delivered = true;
                context.applied = true;
                Transition(if *present { State::present() } else { State::absent() })
            }
            Event::Accept { .. } => Handled,
            Event::Fail { message, feed } if self.admits(*feed) => Transition(State::failed(message.clone())),
            Event::Fail { .. } => Handled,
            Event::Reset if self.singleton => {
                self.drop_value(context);
                self.active = 0;
                Transition(State::opening())
            }
            Event::Reset => Handled,
            Event::BeginFeed => {
                self.active = NEXT_FEED.fetch_add(1, Ordering::Relaxed);
                Handled
            }
            Event::Unopened { feed } if self.admits(Some(*feed)) => {
                self.drop_value(context);
                Transition(State::closed())
            }
            Event::Unopened { .. } => Handled,
            Event::Close => {
                self.drop_value(context);
                Transition(State::closed())
            }
        }
    }

    #[state(superstate = "live")]
    fn opening() -> Outcome<State> {
        Super
    }

    #[state(superstate = "live")]
    fn present() -> Outcome<State> {
        Super
    }

    #[state(superstate = "live")]
    fn absent() -> Outcome<State> {
        Super
    }

    #[state(superstate = "live")]
    fn failed(message: &String) -> Outcome<State> {
        let _ = message;
        Super
    }

    #[state]
    fn closed() -> Outcome<State> {
        Handled
    }
}

/// The one owner of the transitions of an `Entity`: seed, revision order, stale, failure, reset and close. The platform keeps the typed value and shows the verdict.
#[derive(uniffi::Object)]
pub struct EntityMachine {
    flow: Mutex<InitializedStateMachine<Flow>>,
}

impl EntityMachine {
    fn send(&self, event: Event) -> EntityVerdict {
        let mut flow = locked(&self.flow);
        let mut effect = Effect::default();
        flow.handle_with_context(&event, &mut effect);
        let stage = match flow.state() {
            State::Opening {} => EntityStage::Opening,
            State::Present {} => EntityStage::Present,
            State::Absent {} => EntityStage::Absent,
            State::Failed { message } => EntityStage::Failed { message: message.clone() },
            State::Closed {} => EntityStage::Closed,
        };
        let inner = flow.inner();
        EntityVerdict { applied: effect.applied, cleared: effect.cleared, stage, stale: inner.stale, revision: inner.revision, feed: inner.active }
    }
}

#[uniffi::export]
impl EntityMachine {
    #[uniffi::constructor]
    pub fn new(singleton: bool) -> Self {
        Self { flow: Mutex::new(Flow::new(singleton).uninitialized_state_machine().init_with_context(&mut Effect::default())) }
    }

    pub fn seed(&self, revision: i64) -> EntityVerdict { self.send(Event::Seed { revision }) }

    pub fn accept(&self, revision: i64, present: bool, feed: Option<u64>) -> EntityVerdict { self.send(Event::Accept { revision, present, feed }) }

    pub fn fail(&self, message: String, feed: Option<u64>) -> EntityVerdict { self.send(Event::Fail { message, feed }) }

    pub fn reset(&self) -> EntityVerdict { self.send(Event::Reset) }

    pub fn begin_feed(&self) -> EntityVerdict { self.send(Event::BeginFeed) }

    pub fn unopened(&self, feed: u64) -> EntityVerdict { self.send(Event::Unopened { feed }) }

    pub fn database_closed(&self) -> EntityVerdict { self.send(Event::Close) }
}

#[cfg(test)]
#[path = "entity_machine_tests.rs"]
mod entity_machine_tests;
