use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use super::DbError;

type Release = Box<dyn Fn(String) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

enum Hold {
    Pending,
    Open(String),
    Abandoned,
    Released,
}

pub(super) struct TokenLease {
    hold: Mutex<Hold>,
    release: Release,
}

pub(super) fn detached(work: impl Future<Output = ()> + Send + 'static) {
    match super::database::executor() {
        Ok(executor) => {
            executor.spawn(work);
        }
        Err(_) => {
            let _ = std::thread::Builder::new().name("dblayer-release".into()).spawn(move || {
                if let Ok(runtime) = tokio::runtime::Builder::new_current_thread().enable_all().build() {
                    runtime.block_on(work);
                }
            });
        }
    }
}

impl TokenLease {
    pub(super) fn new(release: impl Fn(String) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static) -> Self {
        Self { hold: Mutex::new(Hold::Pending), release: Box::new(release) }
    }

    pub(super) fn opened(&self, token: String) -> Result<(), DbError> {
        let outcome = {
            let mut hold = crate::db::locked(&self.hold);
            match &*hold {
                Hold::Pending => {
                    *hold = Hold::Open(token);
                    return Ok(());
                }
                Hold::Abandoned => {
                    *hold = Hold::Released;
                    Ok(())
                }
                Hold::Open(_) => Err(DbError::Invalid { detail: "the token lease is already open".into() }),
                Hold::Released => Err(DbError::Invalid { detail: "the token lease is already released".into() }),
            }
        };
        self.dispose(token);
        outcome
    }

    pub(super) fn token(&self) -> Option<String> {
        match &*crate::db::locked(&self.hold) {
            Hold::Open(token) => Some(token.clone()),
            _ => None,
        }
    }

    pub(super) fn take_open(&self) -> Option<String> {
        let mut hold = crate::db::locked(&self.hold);
        match &*hold {
            Hold::Open(_) => match std::mem::replace(&mut *hold, Hold::Released) {
                Hold::Open(token) => Some(token),
                _ => None,
            },
            _ => None,
        }
    }

    pub(super) fn abandon(&self) {
        let token = {
            let mut hold = crate::db::locked(&self.hold);
            match &*hold {
                Hold::Pending => {
                    *hold = Hold::Abandoned;
                    None
                }
                Hold::Open(_) => match std::mem::replace(&mut *hold, Hold::Released) {
                    Hold::Open(token) => Some(token),
                    _ => None,
                },
                Hold::Abandoned | Hold::Released => None,
            }
        };
        if let Some(token) = token {
            self.dispose(token);
        }
    }

    fn dispose(&self, token: String) {
        detached((self.release)(token));
    }
}

#[cfg(test)]
#[path = "lease_tests.rs"]
mod lease_tests;
