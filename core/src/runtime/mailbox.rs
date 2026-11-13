use std::collections::VecDeque;
use std::time::Duration;

use super::sync::{lock, AtomicU64, Condvar, Mutex, Ordering, PoisonError};
use crate::blocking::blocking_unit;

pub(crate) struct Mailbox<C> {
    commands: Mutex<VecDeque<C>>,
    wake: Condvar,
    wakeups: AtomicU64,
}

impl<C> Mailbox<C> {
    pub fn new() -> Self { Self { commands: Mutex::new(VecDeque::new()), wake: Condvar::new(), wakeups: AtomicU64::new(0) } }

    pub fn push(&self, command: C) {
        lock(&self.commands).push_back(command);
        crate::lock::effect(|| self.wake.notify_one());
    }

    pub fn drain(&self) -> usize { std::mem::take(&mut *lock(&self.commands)).len() }

    pub fn take(&self, mut next_due: impl FnMut() -> Option<Duration>) -> Vec<C> {
        let mut queue = lock(&self.commands);
        while queue.is_empty() {
            match next_due() {
                None => queue = blocking_unit(|| self.wake.wait(queue).unwrap_or_else(PoisonError::into_inner)),
                Some(left) if left.is_zero() => break,
                Some(left) => queue = blocking_unit(|| self.wake.wait_timeout(queue, left).unwrap_or_else(PoisonError::into_inner).0),
            }
            self.wakeups.fetch_add(1, Ordering::SeqCst);
        }
        std::mem::take(&mut *queue).into()
    }

    #[cfg(test)]
    pub fn inspect<R>(&self, f: impl FnOnce(&VecDeque<C>) -> R) -> R { f(&lock(&self.commands)) }

    pub fn wakeups(&self) -> u64 { self.wakeups.load(Ordering::SeqCst) }
}
