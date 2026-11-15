use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use super::{Cause, Clock, Epoch};
use crate::transport::{GraphQLRequest, PutReply, PutRequest, Response, TransportError, Upstream};

const CLOSED: u64 = 1;
const CUT_SHIFT: u32 = 1;
const CUT_BITS: u64 = 0x7FFF_FFFF;
const EPOCH_SHIFT: u32 = 32;

type Revoked = Arc<dyn Fn(Epoch, String) -> std::pin::Pin<Box<dyn Future<Output = TransportError> + Send>> + Send + Sync>;

/// The gate of the epochs of one database: one atomic value, the epoch above, the `cut` counter in the middle and the closed bit below. The gate owns the transport of the database: the only way to it is a request through the gate, which lends it in the step that checks the epoch.
pub struct EpochGate {
    state: AtomicU64,
    transport: OnceLock<Arc<dyn Upstream>>,
    revoked: OnceLock<Revoked>,
    clock: OnceLock<Arc<dyn Clock>>,
    /// The time before which no request leaves: the `retryAfter` of the last sealed `RATE_LIMITED` answer.
    pause: Mutex<Option<Duration>>,
    /// Test builds record every request that reaches the network after `shutdown`; the gate fails the test that sent it when it is dropped.
    #[cfg(test)]
    recorder: recorder::Recorder,
}

#[cfg(test)]
mod recorder {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    #[derive(Default)]
    pub(super) struct Recorder {
        shut: AtomicBool,
        late: Mutex<Vec<String>>,
    }

    impl Recorder {
        pub(super) fn shut(&self) { self.shut.store(true, Ordering::SeqCst); }

        pub(super) fn sent(&self, what: &str) {
            if self.shut.load(Ordering::SeqCst) { self.late.lock().unwrap_or_else(|error| error.into_inner()).push(what.to_string()); }
        }
    }

    impl Drop for Recorder {
        fn drop(&mut self) {
            let late = std::mem::take(&mut *self.late.lock().unwrap_or_else(|error| error.into_inner()));
            assert!(late.is_empty() || std::thread::panicking(), "requests reached the network after the database was closed: {late:?}");
        }
    }
}

#[derive(Clone, Copy)]
enum Admit {
    Epoch(Epoch),
    Cut(u64),
}

impl Default for EpochGate {
    fn default() -> Self { Self { state: AtomicU64::new(CLOSED), transport: OnceLock::new(), revoked: OnceLock::new(), clock: OnceLock::new(), pause: Mutex::new(None), #[cfg(test)] recorder: recorder::Recorder::default() } }
}

fn epoch_of(state: u64) -> u64 { state >> EPOCH_SHIFT }

fn cut_of(state: u64) -> u64 { (state >> CUT_SHIFT) & CUT_BITS }

fn pack(epoch: u64, cut: u64, closed: bool) -> u64 { (epoch.min(u64::from(u32::MAX)) << EPOCH_SHIFT) | ((cut & CUT_BITS) << CUT_SHIFT) | u64::from(closed) }

impl EpochGate {
    fn update(&self, change: impl Fn(u64) -> u64) {
        let mut seen = self.state.load(Ordering::SeqCst);
        while let Err(now) = self.state.compare_exchange(seen, change(seen), Ordering::SeqCst, Ordering::SeqCst) { seen = now; }
    }

    pub fn attach(&self, transport: Arc<dyn Upstream>) { let _ = self.transport.set(transport); }

    pub(crate) fn attach_revoked(&self, revoked: Revoked) { let _ = self.revoked.set(revoked); }

    pub(crate) fn attach_clock(&self, clock: Arc<dyn Clock>) { let _ = self.clock.set(clock); }

    /// A sealed `RATE_LIMITED` body with `retryAfter` N >= 1 holds every request of the gate until now + N s; the later deadline wins.
    pub(crate) fn note_rate_limit(&self, body: &str) {
        let Some(clock) = self.clock.get() else { return };
        let Ok(body) = serde_json::from_str::<serde_json::Value>(body) else { return };
        let Some(after) = body.get("retryAfter").and_then(serde_json::Value::as_u64).filter(|after| *after >= 1 && body.get("error").and_then(serde_json::Value::as_str) == Some("RATE_LIMITED")) else { return };
        let deadline = clock.now().saturating_add(Duration::from_secs(after));
        let mut pause = crate::lock::lock(&self.pause);
        *pause = Some(pause.map_or(deadline, |held| held.max(deadline)));
    }

    async fn paused(&self) {
        let Some(clock) = self.clock.get() else { return };
        loop {
            let deadline = *crate::lock::lock(&self.pause);
            match deadline { Some(deadline) if clock.now() < deadline => clock.sleep_until(deadline).await, _ => return }
        }
    }

    pub fn has_transport(&self) -> bool { self.transport.get().is_some() }

    fn lend(&self, epoch: Option<Epoch>, variables: Option<&mut serde_json::Value>) -> Result<Arc<dyn Upstream>, TransportError> {
        if epoch.is_some_and(|epoch| !self.admits(epoch)) {
            return Err(TransportError::Interrupted("the epoch ended before the request was sent".into()));
        }
        if let Some(variables) = variables {
            let mut found = false;
            crate::identity::walk_temp_ids(variables, &mut |_| found = true);
            if found { return Err(TransportError::Invalid("the request carries a temporary id".into())); }
        }
        #[cfg(test)]
        self.recorder.sent("lend");
        self.transport.get().cloned().ok_or_else(|| TransportError::Network("the database has no transport".into()))
    }

    /// A request to the network of `epoch` (`None` belongs to no epoch): the epoch check, the borrow of the transport, the call and the first poll of its future run in one synchronous stretch, a closed gate ends the request as `Interrupted` without a transport to call. The borrow lasts as long as the call.
    pub async fn query(&self, epoch: Option<Epoch>, mut request: GraphQLRequest) -> Result<Response, TransportError> { self.paused().await; self.answer(epoch, self.lend(epoch, Some(&mut request.variables))?.query(request).await).await }

    pub async fn mutate(&self, epoch: Option<Epoch>, mut request: GraphQLRequest) -> Result<Response, TransportError> { self.paused().await; self.answer(epoch, self.lend(epoch, Some(&mut request.variables))?.mutate(request).await).await }

    /// The session of `epoch` ended by the server: the one way every origin of the end (HTTP answer, cable close) enters `Session`.
    pub(crate) async fn server_ended(&self, epoch: Epoch, detail: String) -> TransportError {
        match self.revoked.get() {
            Some(revoked) => revoked(epoch, detail).await,
            None => TransportError::Session { code: crate::transport::SessionCode::SessionEnded, detail },
        }
    }

    async fn answer(&self, epoch: Option<Epoch>, result: Result<Response, TransportError>) -> Result<Response, TransportError> {
        if let Err(TransportError::Session { code: crate::transport::SessionCode::RateLimited, detail }) = &result { self.note_rate_limit(detail); }
        match (epoch, result) {
            (Some(epoch), Err(TransportError::Session { code: crate::transport::SessionCode::SessionEnded, detail })) => Err(self.server_ended(epoch, detail).await),
            (_, result) => result,
        }
    }

    pub async fn put(&self, epoch: Option<Epoch>, request: PutRequest) -> Result<PutReply, TransportError> { self.paused().await; self.lend(epoch, None)?.put(request).await }

    pub fn open(&self, epoch: Epoch) { self.update(|state| pack(epoch, cut_of(state), false)); }

    pub fn end(&self, epoch: Epoch, cause: Cause) {
        let grown = u64::from(matches!(cause, Cause::SessionReset | Cause::ServerEnded));
        self.update(|state| pack(epoch_of(state), cut_of(state) + grown, state & CLOSED != 0 || epoch_of(state) == epoch.min(u64::from(u32::MAX))));
    }

    pub fn close(&self, epoch: Epoch) { self.end(epoch, Cause::IdentityChanged); }

    pub fn shutdown(&self) {
        self.update(|state| pack(epoch_of(state), cut_of(state) + 1, true));
        #[cfg(test)]
        self.recorder.shut();
    }

    pub fn cut(&self) -> u64 { cut_of(self.state.load(Ordering::SeqCst)) }

    pub fn admits(&self, epoch: Epoch) -> bool { self.accepts(Admit::Epoch(epoch)) }

    fn accepts(&self, admit: Admit) -> bool {
        let state = self.state.load(Ordering::SeqCst);
        match admit {
            Admit::Epoch(epoch) => state & CLOSED == 0 && epoch_of(state) == epoch.min(u64::from(u32::MAX)),
            Admit::Cut(cut) => cut_of(state) == cut,
        }
    }
}

/// A request to the network of `epoch`: the gate check, the call of the transport method and the first poll of its future run in one synchronous stretch, a closed gate ends the request as `Interrupted` without calling the transport. `None` is a step that belongs to no epoch.
pub async fn send_in_epoch<T, F: Future<Output = Result<T, TransportError>>>(gate: &EpochGate, epoch: Option<Epoch>, request: impl FnOnce() -> F) -> Result<T, TransportError> {
    gate.paused().await;
    if epoch.is_some_and(|epoch| !gate.admits(epoch)) {
        return Err(TransportError::Interrupted("the epoch ended before the request was sent".into()));
    }
    #[cfg(test)]
    gate.recorder.sent("send_in_epoch");
    request().await
}

pub async fn send_in_cut<T, F: Future<Output = Result<T, TransportError>>>(gate: &EpochGate, cut: u64, request: impl FnOnce() -> F) -> Result<Result<T, TransportError>, GateClosed> {
    gate.paused().await;
    if !gate.accepts(Admit::Cut(cut)) { return Err(GateClosed); }
    #[cfg(test)]
    gate.recorder.sent("send_in_cut");
    Ok(request().await)
}

#[derive(Debug, PartialEq, Eq)]
pub struct GateClosed;

#[cfg(test)]
mod tests;
