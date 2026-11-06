use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};

use super::{Cause, Epoch};
use crate::transport::TransportError;

const CLOSED: u64 = 1;
const CUT_SHIFT: u32 = 1;
const CUT_BITS: u64 = 0x7FFF_FFFF;
const EPOCH_SHIFT: u32 = 32;

/// The gate of the epochs of one database: one atomic value, the epoch above, the `cut` counter in the middle and the closed bit below.
pub struct EpochGate(AtomicU64);

#[derive(Clone, Copy)]
enum Admit {
    Epoch(Epoch),
    Cut(u64),
}

impl Default for EpochGate {
    fn default() -> Self { Self(AtomicU64::new(CLOSED)) }
}

fn epoch_of(state: u64) -> u64 { state >> EPOCH_SHIFT }

fn cut_of(state: u64) -> u64 { (state >> CUT_SHIFT) & CUT_BITS }

fn pack(epoch: u64, cut: u64, closed: bool) -> u64 { (epoch.min(u64::from(u32::MAX)) << EPOCH_SHIFT) | ((cut & CUT_BITS) << CUT_SHIFT) | u64::from(closed) }

impl EpochGate {
    fn update(&self, change: impl Fn(u64) -> u64) {
        let mut seen = self.0.load(Ordering::SeqCst);
        while let Err(now) = self.0.compare_exchange(seen, change(seen), Ordering::SeqCst, Ordering::SeqCst) { seen = now; }
    }

    pub fn open(&self, epoch: Epoch) { self.update(|state| pack(epoch, cut_of(state), false)); }

    pub fn end(&self, epoch: Epoch, cause: Cause) {
        let grown = u64::from(matches!(cause, Cause::SessionReset | Cause::ServerEnded));
        self.update(|state| pack(epoch_of(state), cut_of(state) + grown, state & CLOSED != 0 || epoch_of(state) == epoch.min(u64::from(u32::MAX))));
    }

    pub fn close(&self, epoch: Epoch) { self.end(epoch, Cause::IdentityChanged); }

    pub fn shutdown(&self) { self.update(|state| pack(epoch_of(state), cut_of(state) + 1, true)); }

    pub fn cut(&self) -> u64 { cut_of(self.0.load(Ordering::SeqCst)) }

    pub fn admits(&self, epoch: Epoch) -> bool { self.accepts(Admit::Epoch(epoch)) }

    fn accepts(&self, admit: Admit) -> bool {
        let state = self.0.load(Ordering::SeqCst);
        match admit {
            Admit::Epoch(epoch) => state & CLOSED == 0 && epoch_of(state) == epoch.min(u64::from(u32::MAX)),
            Admit::Cut(cut) => cut_of(state) == cut,
        }
    }
}

/// A request to the network of `epoch`: the gate check, the call of the transport method and the first poll of its future run in one synchronous stretch, a closed gate ends the request as `Interrupted` without calling the transport. `None` is a step that belongs to no epoch.
pub async fn send_in_epoch<T, F: Future<Output = Result<T, TransportError>>>(gate: &EpochGate, epoch: Option<Epoch>, request: impl FnOnce() -> F) -> Result<T, TransportError> {
    if epoch.is_some_and(|epoch| !gate.admits(epoch)) {
        return Err(TransportError::Interrupted("the epoch ended before the request was sent".into()));
    }
    request().await
}

pub async fn send_in_cut<T, F: Future<Output = Result<T, TransportError>>>(gate: &EpochGate, cut: u64, request: impl FnOnce() -> F) -> Result<Result<T, TransportError>, GateClosed> {
    if !gate.accepts(Admit::Cut(cut)) { return Err(GateClosed); }
    Ok(request().await)
}

#[derive(Debug, PartialEq, Eq)]
pub struct GateClosed;

#[cfg(test)]
#[path = "gate_tests.rs"]
mod tests;
