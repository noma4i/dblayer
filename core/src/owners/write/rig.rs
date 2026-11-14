//! Rig of the executor tests: a manual task spawner and a transport stub for owners on `DetScheduler`.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};

use async_trait::async_trait;

use super::wire::{Running, Task, Tasks};
use crate::spec::support::{new_log, Log, Silent, Sink};
use crate::runtime::{Address, DetScheduler, StateOwner};
use crate::transport::{GraphQLRequest, PutRequest, PutReply, Response, Upstream, TransportError};

pub(super) const STEPS: usize = 20_000;

/// A peer owner that keeps the `Debug` form of what it receives. The address is the address of `P` for the owners that send to it.
pub(super) fn peer<P: StateOwner>(det: &mut DetScheduler, name: &'static str) -> (Address<P>, Log<String>) {
    let log = new_log();
    let sink = det.add::<Sink<P>>(name, log.clone(), Silent);
    (Address::new(sink.index()), log)
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> { mutex.lock().unwrap_or_else(PoisonError::into_inner) }


#[derive(Default)]
struct Pool {
    next: u64,
    reject_blocking: bool,
    tasks: BTreeMap<u64, Task>,
    jobs: BTreeMap<u64, Box<dyn FnOnce() + Send>>,
}

/// Runs tasks and blocking jobs when the test asks, in the order they were spawned. A cancelled task is dropped without another poll.
#[derive(Clone, Default)]
pub(super) struct ManualSpawn(Arc<Mutex<Pool>>);

impl ManualSpawn {
    pub fn reject_next_blocking(&self) { lock(&self.0).reject_blocking = true; }
    pub fn pending(&self) -> usize {
        let pool = lock(&self.0);
        pool.tasks.len() + pool.jobs.len()
    }

    fn poll_all(&self) -> bool {
        let waker = Waker::noop().clone();
        let mut moved = false;
        let jobs = std::mem::take(&mut lock(&self.0).jobs);
        for (_, job) in jobs {
            job();
            moved = true;
        }
        let ids: Vec<u64> = lock(&self.0).tasks.keys().copied().collect();
        for id in ids {
            let Some(mut task) = lock(&self.0).tasks.remove(&id) else { continue };
            match task.as_mut().poll(&mut Context::from_waker(&waker)) {
                Poll::Ready(()) => moved = true,
                Poll::Pending => {
                    lock(&self.0).tasks.insert(id, task);
                }
            }
        }
        moved
    }
}

impl Tasks for ManualSpawn {
    fn start(&self, _watch: super::wire::FailureWatch<super::transfer::Transfers>, task: Task) -> Running {
        let mut pool = lock(&self.0);
        let id = pool.next;
        pool.next += 1;
        pool.tasks.insert(id, task);
        let pool = self.0.clone();
        Running::new(move || {
            let dropped = lock(&pool).tasks.remove(&id);
            drop(dropped);
        })
    }

    fn start_blocking(&self, _watch: super::wire::FailureWatch<super::transfer::Transfers>, job: Box<dyn FnOnce() + Send>) -> Running {
        let mut pool = lock(&self.0);
        if std::mem::take(&mut pool.reject_blocking) {
            drop(pool);
            drop(job);
            return Running::new(|| {});
        }
        let id = pool.next;
        pool.next += 1;
        pool.jobs.insert(id, job);
        let pool = self.0.clone();
        Running::new(move || {
            let dropped = lock(&pool).jobs.remove(&id);
            drop(dropped);
        })
    }
}

#[derive(Clone)]
pub(super) enum Act {
    Reply(Result<Response, TransportError>),
    Hang,
}

/// Scripted answers, the requests it saw and the count of requests dropped while they waited.
#[derive(Clone, Default)]
pub(super) struct Stub {
    script: Arc<Mutex<VecDeque<Act>>>,
    calls: Arc<Mutex<Vec<GraphQLRequest>>>,
    dropped: Arc<AtomicUsize>,
    /// The tokens of the PUT requests it saw, and whether a PUT hangs until the token is cancelled.
    puts: Arc<Mutex<Vec<crate::transport::CancelToken>>>,
    hang_puts: Arc<std::sync::atomic::AtomicBool>,
    signal: Arc<tokio::sync::Notify>,
}

struct Waiting(Arc<AtomicUsize>, Arc<tokio::sync::Notify>);

impl Drop for Waiting {
    fn drop(&mut self) { self.0.fetch_add(1, Ordering::SeqCst); crate::lock::effect(|| self.1.notify_waiters()); }
}

impl Stub {
    pub fn script(&self, acts: impl IntoIterator<Item = Act>) { lock(&self.script).extend(acts); }

    pub fn calls(&self) -> Vec<GraphQLRequest> { lock(&self.calls).clone() }

    pub fn dropped(&self) -> usize { self.dropped.load(Ordering::SeqCst) }

    pub fn put_tokens(&self) -> Vec<crate::transport::CancelToken> { lock(&self.puts).clone() }

    /// Notified when a PUT arrives and when a waiting request is dropped.
    pub fn signal(&self) -> &tokio::sync::Notify { &self.signal }

    pub fn hang_puts(&self) { self.hang_puts.store(true, Ordering::SeqCst); }

    async fn answer(&self, request: GraphQLRequest) -> Result<Response, TransportError> {
        lock(&self.calls).push(request);
        let act = lock(&self.script).pop_front().unwrap_or(Act::Reply(Ok(Response::data(serde_json::json!({"ok": true})))));
        match act {
            Act::Reply(result) => result,
            Act::Hang => {
                let _waiting = Waiting(self.dropped.clone(), self.signal.clone());
                std::future::pending::<()>().await;
                unreachable!("a hanging request never ends")
            }
        }
    }
}

#[async_trait]
impl Upstream for Stub {
    async fn query(&self, request: GraphQLRequest) -> Result<Response, TransportError> { self.answer(request).await }

    async fn mutate(&self, request: GraphQLRequest) -> Result<Response, TransportError> { self.answer(request).await }


    async fn put(&self, request: PutRequest) -> Result<PutReply, TransportError> {
        lock(&self.puts).push(request.cancel.clone());
        crate::lock::effect(|| self.signal.notify_waiters());
        if self.hang_puts.load(Ordering::SeqCst) {
            let _waiting = Waiting(self.dropped.clone(), self.signal.clone());
            std::future::pending::<()>().await;
        }
        Ok(PutReply::with_etag("etag"))
    }
}

pub(super) fn open_gate(epoch: u64, transport: Arc<dyn Upstream>) -> Arc<crate::runtime::EpochGate> {
    let gate = Arc::new(crate::runtime::EpochGate::default());
    gate.open(epoch);
    gate.attach(transport);
    gate
}

/// Runs tasks and jobs until nothing moves, with the owners between. `wake` posts a no-op input: the scheduler takes what a task
/// replied only after a step, so a reply from a task needs one step to be seen. A run that does not settle is a failure.
pub(super) fn settle(det: &mut DetScheduler, spawn: &ManualSpawn, wake: impl Fn(&mut DetScheduler)) {
    for _ in 0..STEPS {
        let polled = spawn.poll_all();
        if polled {
            wake(det);
        }
        if !(det.step() || polled) {
            return;
        }
    }
    panic!("the owners did not settle in {STEPS} rounds");
}
