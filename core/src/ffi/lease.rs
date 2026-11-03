use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use statig::prelude::*;
use tokio::runtime::Handle;

use super::DbError;

type Release = Box<dyn Fn(String) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

enum Signal {
    Opened(String),
    BeginClose,
    FinishClose,
    RestoreClose,
    Abandon,
}

struct Reply {
    result: Result<(), DbError>,
    taken: Option<String>,
    dispose: Option<String>,
}

impl Reply {
    fn new() -> Self {
        Self { result: Ok(()), taken: None, dispose: None }
    }

    fn refuse(&mut self, detail: &str, token: &str, dispose: bool) {
        self.result = Err(DbError::Invalid { detail: detail.into() });
        self.dispose = dispose.then(|| token.to_string());
    }
}

struct Hold;

#[state_machine(initial = "State::pending()")]
impl Hold {
    #[state]
    fn pending(event: &Signal) -> Outcome<State> {
        match event {
            Signal::Opened(token) => Transition(State::open(token.clone())),
            Signal::Abandon => Transition(State::abandoned()),
            _ => Handled,
        }
    }

    #[state]
    fn open(token: &String, context: &mut Reply, event: &Signal) -> Outcome<State> {
        match event {
            Signal::Opened(next) if next == token => {
                context.refuse("the token lease already holds this token", next, false);
                Handled
            }
            Signal::Opened(next) => {
                context.refuse("the token lease is already open", next, true);
                Handled
            }
            Signal::BeginClose => {
                context.taken = Some(token.clone());
                Transition(State::closing(token.clone()))
            }
            Signal::Abandon => {
                context.dispose = Some(token.clone());
                Transition(State::released(token.clone()))
            }
            Signal::FinishClose | Signal::RestoreClose => Handled,
        }
    }

    #[state]
    fn closing(token: &String, context: &mut Reply, event: &Signal) -> Outcome<State> {
        match event {
            Signal::Opened(next) if next == token => {
                context.refuse("the token lease already holds this token", next, false);
                Handled
            }
            Signal::Opened(next) => {
                context.refuse("the token lease is already open", next, true);
                Handled
            }
            Signal::FinishClose => Transition(State::released(token.clone())),
            Signal::RestoreClose => Transition(State::open(token.clone())),
            Signal::BeginClose | Signal::Abandon => Handled,
        }
    }

    #[state]
    fn abandoned(context: &mut Reply, event: &Signal) -> Outcome<State> {
        match event {
            Signal::Opened(token) => {
                context.dispose = Some(token.clone());
                Transition(State::released(token.clone()))
            }
            _ => Handled,
        }
    }

    #[state]
    fn released(token: &String, context: &mut Reply, event: &Signal) -> Outcome<State> {
        if let Signal::Opened(next) = event {
            if next == token {
                context.refuse("the token lease already holds this token", next, false);
            } else {
                context.refuse("the token lease is already released", next, true);
            }
        }
        Handled
    }
}

pub(super) struct TokenLease {
    hold: Mutex<InitializedStateMachine<Hold>>,
    handle: Handle,
    release: Release,
}

impl TokenLease {
    pub(super) fn new(handle: Handle, release: impl Fn(String) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static) -> Self {
        Self { hold: Mutex::new(Hold.uninitialized_state_machine().init_with_context(&mut Reply::new())), handle, release: Box::new(release) }
    }

    pub(super) fn opened(&self, token: String) -> Result<(), DbError> {
        self.signal(Signal::Opened(token)).result
    }

    pub(super) fn token(&self) -> Option<String> {
        match crate::db::locked(&self.hold).state() {
            State::Open { token } => Some(token.clone()),
            _ => None,
        }
    }

    pub(super) fn begin_close(&self) -> Option<String> {
        self.signal(Signal::BeginClose).taken
    }

    pub(super) fn finish_close(&self) {
        self.signal(Signal::FinishClose);
    }

    pub(super) fn restore_close(&self) {
        self.signal(Signal::RestoreClose);
    }

    pub(super) fn abandon(&self) {
        self.signal(Signal::Abandon);
    }

    fn signal(&self, signal: Signal) -> Reply {
        let mut reply = Reply::new();
        crate::db::locked(&self.hold).handle_with_context(&signal, &mut reply);
        if let Some(token) = reply.dispose.take() {
            crate::task::spawn_on_unpinned(&self.handle, (self.release)(token));
        }
        reply
    }
}

#[cfg(test)]
#[path = "lease_tests.rs"]
mod lease_tests;
