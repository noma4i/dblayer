use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::sync::atomic::{AtomicUsize, Ordering};


use super::dispatch::{mark_core_thread, Dispatch};
use super::error::DbError;
use super::open::Shared;
use super::watch::{Evaluated, Loaded, Version};

type ReadRun = Box<dyn FnOnce(&Shared) -> Result<ReadRows, DbError> + Send>;

const READERS: usize = 2;

pub(crate) enum ReadTarget {
    Publish { key: String, token: u64, previous: Arc<Version>, grow: Option<(usize, usize)> },
    Join { key: String, token: u64, joiners: Vec<u64> },
    Open { key: String, token: u64 },
}

pub(crate) enum ReadRows {
    Skipped { revision: i64, epoch: u64 },
    Evaluated(Box<Evaluated>),
    Whole(Loaded),
    Moved,
}

pub(crate) enum ReadResult {
    ReadOk { target: ReadTarget, rows: ReadRows },
    ReadFailed { target: ReadTarget, error: DbError },
}

impl ReadResult {
    pub(crate) fn key(&self) -> &str {
        let (ReadResult::ReadOk { target, .. } | ReadResult::ReadFailed { target, .. }) = self;
        match target {
            ReadTarget::Publish { key, .. } | ReadTarget::Join { key, .. } | ReadTarget::Open { key, .. } => key,
        }
    }
}

pub(crate) struct ReadJob {
    pub target: ReadTarget,
    pub run: ReadRun,
}

pub(crate) struct ReadPermit(Arc<AtomicUsize>);

impl Drop for ReadPermit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(crate) struct CompletedRead {
    pub result: ReadResult,
    pub permit: ReadPermit,
}

#[derive(Default)]
struct Queue {
    jobs: VecDeque<(ReadJob, ReadPermit)>,
    closed: bool,
}

pub(crate) struct ReadPool {
    queue: Mutex<Queue>,
    wake: Condvar,
    dispatch: Arc<Dispatch>,
    outstanding: Arc<AtomicUsize>,
    #[cfg(test)]
    pub(crate) inline: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pub(crate) ran_on: Mutex<Vec<std::thread::ThreadId>>,
}

fn held<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl ReadPool {
    pub(crate) fn new(dispatch: Arc<Dispatch>) -> Arc<Self> {
        Arc::new(Self {
            queue: Mutex::new(Queue::default()),
            wake: Condvar::new(),
            dispatch,
            outstanding: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            inline: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            ran_on: Mutex::new(Vec::new()),
        })
    }

    pub(crate) fn start(pool: &Arc<ReadPool>, shared: std::sync::Weak<Shared>) -> Result<(), DbError> {
        for _ in 0..READERS {
            let (pool, shared) = (pool.clone(), shared.clone());
            super::threads::spawn_named(crate::task::Carry::Detached, "dblayer-read", move || pool.work(&shared))?;
        }
        Ok(())
    }

    pub(crate) fn has_capacity(&self) -> bool {
        self.outstanding.load(Ordering::Acquire) < READERS
    }

    pub(crate) fn submit(&self, shared: &Shared, job: ReadJob) -> bool {
        if self.outstanding.try_update(Ordering::AcqRel, Ordering::Acquire, |count| (count < READERS).then_some(count + 1)).is_err() {
            return false;
        }
        let permit = ReadPermit(self.outstanding.clone());
        #[cfg(test)]
        if self.inline.load(std::sync::atomic::Ordering::SeqCst) {
            let result = Self::run(shared, job);
            self.dispatch.push_read(CompletedRead { result, permit });
            return true;
        }
        let _ = shared;
        held(&self.queue).jobs.push_back((job, permit));
        self.wake.notify_one(); // lock-ok: queue guard ended at statement
        true
    }

    fn run(shared: &Shared, job: ReadJob) -> ReadResult {
        let ReadJob { target, run } = job;
        #[cfg(test)]
        held(&shared.reads.ran_on).push(std::thread::current().id());
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(shared))) {
            Ok(Ok(rows)) => ReadResult::ReadOk { target, rows },
            Ok(Err(error)) => ReadResult::ReadFailed { target, error },
            Err(_) => ReadResult::ReadFailed { target, error: DbError::Internal("a read panicked".to_string()) },
        }
    }

    fn work(&self, shared: &std::sync::Weak<Shared>) {
        mark_core_thread();
        loop {
            let job = {
                let mut queue = held(&self.queue);
                loop {
                    if let Some(job) = queue.jobs.pop_front() {
                        break Some(job);
                    }
                    if queue.closed {
                        break None;
                    }
                    queue = self.wake.wait(queue).unwrap_or_else(|poisoned| poisoned.into_inner());
                }
            };
            let Some((job, permit)) = job else { return };
            match shared.upgrade() {
                Some(shared) => self.dispatch.push_read(CompletedRead { result: Self::run(&shared, job), permit }),
                None => self.dispatch.progress.advance(),
            }
        }
    }

    pub(crate) fn close(&self) {
        held(&self.queue).closed = true;
        self.wake.notify_all();
    }
}
