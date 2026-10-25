use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use serde_json::{json, Value};
use statig::prelude::*;
use tokio::sync::mpsc;
use crate::pacing::{Backoff, Clock, RetryPolicy};
use crate::transport::{Connectivity, GraphQLRequest, SessionCode, SubscriptionClose, SubscriptionEvent, SubscriptionFailure, SubscriptionFault, SubscriptionRetry, TransportError};

const SILENCE: Duration = Duration::from_secs(25);
const PONG_WAIT: Duration = Duration::from_secs(20);

pub(super) struct Entry {
    pub(super) request: GraphQLRequest,
    pub(super) events: mpsc::UnboundedSender<SubscriptionEvent>,
}

pub(super) struct Core {
    pub(super) entries: BTreeMap<String, Entry>,
    pub(super) frames: Vec<Value>,
    clock: Arc<dyn Clock>,
    online: Arc<Connectivity>,
    last_traffic: Duration,
    ping_ts: u64,
    retry: SubscriptionRetry,
    backoff: Backoff,
}

impl Core {
    pub(super) fn frame(&self, id: &str) -> Option<Value> {
        self.entries.get(id).map(|entry| json!({
            "t":"subscribe", "id":id, "query":entry.request.document,
            "variables":entry.request.variables, "operationName":entry.request.operation_name,
        }))
    }

    fn attach(&self) -> State {
        if self.online.is_online() { State::connecting() } else { State::offline() }
    }

    fn after_failure(&mut self) -> State {
        if self.entries.is_empty() { return State::idle(); }
        if !self.online.is_online() { return State::offline(); }
        self.retry = self.retry.record_failure(SubscriptionFault::Transient).state;
        State::backoff(self.clock.now() + self.backoff.delay(self.retry.attempts.saturating_sub(1)))
    }

    fn heard(&mut self, confirms: bool) {
        if confirms { self.retry = self.retry.confirmed(); }
        self.last_traffic = self.clock.now();
    }
}

fn terminal(code: u16, reason: &str) -> Option<SubscriptionFailure> {
    let session = match (code, reason) {
        (_, "LOGGED_OUT") => SessionCode::SessionEnded,
        (4401, _) => SessionCode::Unauthenticated,
        (4403, _) | (_, "ACCOUNT_LOCKED") => SessionCode::AccountUnavailable,
        _ => return None,
    };
    let mut failure = SubscriptionFailure::from_error(TransportError::Session { code: session, detail: reason.into() });
    failure.close = Some(SubscriptionClose { code, terminal: true, session: Some(session) });
    Some(failure)
}

pub(super) enum Heard {
    Subscribed,
    Next,
    Pong(u64),
    Other,
}

pub(super) enum Event {
    Demand,
    Connectivity,
    Resume,
    Refuse(SubscriptionFailure),
    Opened,
    OpenFailed,
    Closed { code: u16, reason: String },
    Lost,
    Sent,
    Tick,
    Heard(Heard),
}

struct Transitions;

#[state_machine(initial = "State::idle()")]
impl Transitions {
    #[state]
    fn idle(context: &mut Core, event: &Event) -> Outcome<State> {
        match event {
            Event::Demand if !context.entries.is_empty() => Transition(context.attach()),
            _ => Handled,
        }
    }

    #[state]
    fn refused(failure: &SubscriptionFailure, event: &Event) -> Outcome<State> {
        let _ = failure;
        match event {
            Event::Resume => Transition(State::idle()),
            _ => Handled,
        }
    }

    #[superstate]
    fn live(context: &mut Core, event: &Event) -> Outcome<State> {
        match event {
            Event::Demand if context.entries.is_empty() => Transition(State::idle()),
            Event::Refuse(failure) => Transition(State::refused(failure.clone())),
            _ => Handled,
        }
    }

    #[state(superstate = "live")]
    fn offline(context: &mut Core, event: &Event) -> Outcome<State> {
        match event {
            Event::Connectivity if context.online.is_online() => Transition(State::connecting()),
            Event::Connectivity => Handled,
            _ => Super,
        }
    }

    #[state(superstate = "live")]
    fn backoff(context: &mut Core, event: &Event, until: &Duration) -> Outcome<State> {
        match event {
            Event::Tick if context.clock.now() >= *until => Transition(State::connecting()),
            Event::Tick => Handled,
            Event::Connectivity if !context.online.is_online() => Transition(State::offline()),
            Event::Connectivity => Handled,
            _ => Super,
        }
    }

    #[superstate(superstate = "live")]
    fn attached(context: &mut Core, event: &Event) -> Outcome<State> {
        match event {
            Event::Connectivity if !context.online.is_online() => Transition(State::offline()),
            Event::Connectivity => Handled,
            Event::Closed { code, reason } => Transition(match terminal(*code, reason) {
                Some(failure) => State::refused(failure),
                None => context.after_failure(),
            }),
            _ => Super,
        }
    }

    #[state(superstate = "attached")]
    fn connecting(context: &mut Core, event: &Event) -> Outcome<State> {
        match event {
            Event::Opened => Transition(State::open(None)),
            Event::OpenFailed => Transition(context.after_failure()),
            _ => Super,
        }
    }

    #[action]
    fn enter_open(context: &mut Core) {
        context.last_traffic = context.clock.now();
        context.frames = context.entries.keys().filter_map(|id| context.frame(id)).collect();
    }

    #[state(superstate = "attached", entry_action = "enter_open")]
    fn open(context: &mut Core, event: &Event, pong_due: &mut Option<Duration>) -> Outcome<State> {
        match event {
            Event::Sent => {
                context.last_traffic = context.clock.now();
                Handled
            }
            Event::Heard(Heard::Subscribed) => {
                context.heard(true);
                Handled
            }
            Event::Heard(Heard::Next) => {
                *pong_due = None;
                context.heard(true);
                Handled
            }
            Event::Heard(Heard::Pong(ts)) if pong_due.is_some() && *ts == context.ping_ts => {
                *pong_due = None;
                context.heard(true);
                Handled
            }
            Event::Heard(Heard::Pong(_)) => Handled,
            Event::Heard(Heard::Other) => {
                context.heard(false);
                Handled
            }
            Event::Lost => Transition(context.after_failure()),
            Event::Tick => {
                let now = context.clock.now();
                if now < pong_due.unwrap_or(context.last_traffic + SILENCE) { return Handled; }
                if pong_due.is_some() { return Transition(context.after_failure()); }
                *pong_due = Some(now + PONG_WAIT);
                context.ping_ts = context.ping_ts.wrapping_add(1);
                context.frames.push(json!({"t":"ping", "ts":context.ping_ts}));
                Handled
            }
            _ => Super,
        }
    }
}

pub(super) struct Lifecycle {
    machine: InitializedStateMachine<Transitions>,
    pub(super) core: Core,
}

impl Lifecycle {
    pub(super) fn new(clock: Arc<dyn Clock>, online: Arc<Connectivity>) -> Self {
        let mut core = Core {
            entries: BTreeMap::new(),
            frames: Vec::new(),
            last_traffic: clock.now(),
            clock,
            online,
            ping_ts: 0,
            retry: SubscriptionRetry::default(),
            backoff: RetryPolicy::Subscription.backoff().randomized(),
        };
        let machine = Transitions.uninitialized_state_machine().init_with_context(&mut core);
        Self { machine, core }
    }

    pub(super) fn handle(&mut self, event: Event) {
        self.machine.handle_with_context(&event, &mut self.core);
        if let State::Refused { failure } = self.machine.state() {
            for (_, entry) in std::mem::take(&mut self.core.entries) {
                let _ = entry.events.send(SubscriptionEvent::Failed(failure.clone()));
            }
        }
    }

    pub(super) fn refusal(&self) -> Option<&SubscriptionFailure> {
        match self.machine.state() {
            State::Refused { failure } => Some(failure),
            _ => None,
        }
    }

    pub(super) fn is_connecting(&self) -> bool { matches!(self.machine.state(), State::Connecting {}) }

    pub(super) fn is_open(&self) -> bool { matches!(self.machine.state(), State::Open { .. }) }

    pub(super) fn deadline(&self) -> Option<Duration> {
        match self.machine.state() {
            State::Backoff { until } => Some(*until),
            State::Open { pong_due } => Some(pong_due.unwrap_or(self.core.last_traffic + SILENCE)),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(super) fn now(&self) -> Duration { self.core.clock.now() }
}
