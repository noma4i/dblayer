use tokio::sync::watch;

/// The one reader of a `watch` value: its first `next` returns the current value, each later one the newest value that is newer than the last it returned. A slow reader skips the values between, never the last one.
pub(super) struct Reader<T> { changes: tokio::sync::Mutex<Reading<T>>, stopped: watch::Sender<bool> }

struct Reading<T> { changes: watch::Receiver<T>, first: bool }

impl<T: Clone> Reader<T> {
    pub(super) fn of(changes: watch::Receiver<T>) -> Self {
        let (stopped, _) = watch::channel(false);
        Self { changes: tokio::sync::Mutex::new(Reading { changes, first: true }), stopped }
    }

    /// The next value, `None` once the reader was cancelled or the source is gone.
    pub(super) async fn next(&self) -> Option<T> {
        let mut stopped = self.stopped.subscribe();
        let mut reading = self.changes.lock().await;
        if *stopped.borrow() { return None; }
        if std::mem::take(&mut reading.first) { return Some(reading.changes.borrow_and_update().clone()); }
        tokio::select! {
            biased;
            _ = async { let _ = stopped.wait_for(|stopped| *stopped).await; } => None,
            changed = reading.changes.changed() => changed.ok().filter(|_| !*self.stopped.borrow()).map(|()| reading.changes.borrow_and_update().clone()),
        }
    }

    /// Ends this reader: a waiting `next` and every later one return `None`.
    pub(super) fn cancel(&self) { crate::lock::effect(|| self.stopped.send_replace(true)); }
}
