use std::time::Duration;

use statig::prelude::*;

use crate::pacing::Backoff;
use crate::transport::SubscriptionFailureKind;

const DEGRADED_AFTER: u32 = 4;

pub(super) enum Event {
    Connected,
    Ready,
    Next,
    Lost { kind: SubscriptionFailureKind, online: bool },
    Resumed,
}

pub(super) enum Step {
    Open,
    Wait(Duration),
    Stop,
}

struct Core {
    attempts: u32,
    degraded: bool,
    reload: bool,
    backoff: Backoff,
}

struct Transitions;

#[state_machine(initial = "State::connecting()")]
impl Transitions {
    #[superstate]
    fn lifecycle(context: &mut Core, event: &Event) -> Outcome<State> {
        match event {
            Event::Lost { kind: SubscriptionFailureKind::Transport, online: false } => Transition(State::offline()),
            Event::Lost { kind: SubscriptionFailureKind::Transport, online: true } => {
                context.attempts += 1;
                context.degraded |= context.attempts >= DEGRADED_AFTER;
                Transition(State::waiting(context.backoff.delay(context.attempts - 1)))
            }
            Event::Lost { .. } => {
                context.degraded = true;
                Transition(State::stopped())
            }
            _ => Handled,
        }
    }

    #[state(superstate = "lifecycle")]
    fn connecting(event: &Event) -> Outcome<State> {
        match event {
            Event::Connected => Transition(State::joined()),
            _ => Super,
        }
    }

    #[state(superstate = "lifecycle")]
    fn joined(event: &Event) -> Outcome<State> {
        match event {
            Event::Ready => Transition(State::ready()),
            _ => Super,
        }
    }

    #[superstate(superstate = "lifecycle")]
    fn open(context: &mut Core, event: &Event) -> Outcome<State> {
        match event {
            Event::Ready => {
                context.reload = true;
                Handled
            }
            _ => Super,
        }
    }

    #[state(superstate = "open")]
    fn ready(context: &mut Core, event: &Event) -> Outcome<State> {
        match event {
            Event::Next => {
                context.attempts = 0;
                context.degraded = false;
                Transition(State::streaming())
            }
            _ => Super,
        }
    }

    #[state(superstate = "open")]
    fn streaming(event: &Event) -> Outcome<State> {
        match event {
            Event::Next => Handled,
            _ => Super,
        }
    }

    #[state]
    fn waiting(delay: &Duration, event: &Event) -> Outcome<State> {
        let _ = delay;
        match event {
            Event::Resumed => Transition(State::connecting()),
            _ => Handled,
        }
    }

    #[state]
    fn offline(event: &Event) -> Outcome<State> {
        match event {
            Event::Resumed => Transition(State::connecting()),
            _ => Handled,
        }
    }

    #[state]
    fn stopped() -> Outcome<State> {
        Handled
    }
}

pub(super) struct Lifecycle {
    machine: InitializedStateMachine<Transitions>,
    core: Core,
}

impl Lifecycle {
    pub(super) fn new(backoff: Backoff) -> Self {
        let mut core = Core { attempts: 0, degraded: false, reload: false, backoff };
        let machine = Transitions.uninitialized_state_machine().init_with_context(&mut core);
        Self { machine, core }
    }

    pub(super) fn handle(&mut self, event: Event) {
        self.machine.handle_with_context(&event, &mut self.core);
    }

    pub(super) fn degraded(&self) -> bool {
        self.core.degraded
    }

    pub(super) fn stopped(&self) -> bool {
        matches!(self.machine.state(), State::Stopped {})
    }

    pub(super) fn take_reload(&mut self) -> bool {
        std::mem::take(&mut self.core.reload)
    }

    pub(super) fn step(&self) -> Step {
        match self.machine.state() {
            State::Stopped {} => Step::Stop,
            State::Waiting { delay } => Step::Wait(*delay),
            _ => Step::Open,
        }
    }
}
