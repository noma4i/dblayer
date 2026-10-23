use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use tokio::runtime::Handle;

use super::DbError;

type Release = Box<dyn Fn(String) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

enum Hold {
    Pending,
    Open(String),
    Closing(String),
    Abandoned,
    Released(Option<String>),
}

pub(super) struct TokenLease {
    hold: Mutex<Hold>,
    handle: Handle,
    release: Release,
}

impl TokenLease {
    pub(super) fn new(handle: Handle, release: impl Fn(String) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static) -> Self {
        Self { hold: Mutex::new(Hold::Pending), handle, release: Box::new(release) }
    }

    pub(super) fn opened(&self, token: String) -> Result<(), DbError> {
        let (outcome, dispose) = {
            let mut hold = crate::db::locked(&self.hold);
            match &*hold {
                Hold::Pending => {
                    *hold = Hold::Open(token);
                    return Ok(());
                }
                Hold::Abandoned => {
                    *hold = Hold::Released(Some(token.clone()));
                    (Ok(()), true)
                }
                Hold::Open(held) | Hold::Closing(held) | Hold::Released(Some(held)) if *held == token => {
                    (Err(DbError::Invalid { detail: "the token lease already holds this token".into() }), false)
                }
                Hold::Open(_) | Hold::Closing(_) => (Err(DbError::Invalid { detail: "the token lease is already open".into() }), true),
                Hold::Released(_) => (Err(DbError::Invalid { detail: "the token lease is already released".into() }), true),
            }
        };
        if dispose {
            self.dispose(token);
        }
        outcome
    }

    pub(super) fn token(&self) -> Option<String> {
        match &*crate::db::locked(&self.hold) {
            Hold::Open(token) => Some(token.clone()),
            _ => None,
        }
    }

    pub(super) fn begin_close(&self) -> Option<String> {
        let mut hold = crate::db::locked(&self.hold);
        match std::mem::replace(&mut *hold, Hold::Abandoned) {
            Hold::Open(token) => {
                *hold = Hold::Closing(token.clone());
                Some(token)
            }
            other => {
                *hold = other;
                None
            }
        }
    }

    pub(super) fn finish_close(&self) {
        self.settle_close(|token| Hold::Released(Some(token)));
    }

    pub(super) fn restore_close(&self) {
        self.settle_close(Hold::Open);
    }

    fn settle_close(&self, next: impl FnOnce(String) -> Hold) {
        let mut hold = crate::db::locked(&self.hold);
        match std::mem::replace(&mut *hold, Hold::Abandoned) {
            Hold::Closing(token) => *hold = next(token),
            other => *hold = other,
        }
    }

    pub(super) fn abandon(&self) {
        let token = {
            let mut hold = crate::db::locked(&self.hold);
            match std::mem::replace(&mut *hold, Hold::Abandoned) {
                Hold::Pending | Hold::Abandoned => None,
                Hold::Open(token) => {
                    *hold = Hold::Released(Some(token.clone()));
                    Some(token)
                }
                other => {
                    *hold = other;
                    None
                }
            }
        };
        if let Some(token) = token {
            self.dispose(token);
        }
    }

    fn dispose(&self, token: String) {
        self.handle.spawn((self.release)(token));
    }
}

#[cfg(test)]
#[path = "lease_tests.rs"]
mod lease_tests;
