use tokio::sync::watch;

/// The one reader of a `watch` value: its first `next` returns the current value, each later one the newest value that is newer than the last it returned. A slow reader skips the values between, never the last one. A poll reads the value itself, the task of the core runtime only waits for a change or the stop: the continuation of the platform is woken from a thread of the runtime, never from the thread that wrote the value.
pub(super) struct Reader<T> { reading: std::sync::Mutex<Reading<T>>, stopped: watch::Sender<bool> }

struct Reading<T> { changes: watch::Receiver<T>, first: bool }

impl<T: Clone + Send + Sync + 'static> Reader<T> {
    pub(super) fn of(changes: watch::Receiver<T>) -> Self {
        let (stopped, _) = watch::channel(false);
        Self { reading: std::sync::Mutex::new(Reading { changes, first: true }), stopped }
    }

    pub(super) async fn next(&self) -> Option<T> {
        poll_then_wait(|| {
            let mut reading = crate::lock::lock(&self.reading);
            if *self.stopped.borrow() { return std::task::Poll::Ready(None); }
            match (std::mem::take(&mut reading.first), reading.changes.has_changed()) {
                (_, Err(_)) => std::task::Poll::Ready(None),
                (true, _) | (_, Ok(true)) => std::task::Poll::Ready(Some(reading.changes.borrow_and_update().clone())),
                _ => std::task::Poll::Pending,
            }
        }, || {
            let (mut changes, mut stopped) = (crate::lock::lock(&self.reading).changes.clone(), self.stopped.subscribe());
            Box::pin(super::database::on_core(super::database::Scope::root(), async move {
                tokio::select! {
                    biased;
                    _ = stopped.wait_for(|stopped| *stopped) => {}
                    _ = changes.changed() => {}
                }
            }))
        }).await
    }

    pub(super) fn cancel(&self) { crate::lock::effect(|| self.stopped.send_replace(true)); }
}

/// Every poll first reads the value that is published (ready at once), only the wait for a new value runs as a task of the core runtime. A wait that fails ends the reading.
pub(super) async fn poll_then_wait<T>(mut read: impl FnMut() -> std::task::Poll<Option<T>>, mut wait: impl FnMut() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), super::DbError>> + Send>>) -> Option<T> {
    let mut waiting: Option<std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), super::DbError>> + Send>>> = None;
    std::future::poll_fn(|cx| loop {
        if let std::task::Poll::Ready(value) = read() { return std::task::Poll::Ready(value); }
        let pending = waiting.get_or_insert_with(&mut wait);
        match pending.as_mut().poll(cx) {
            std::task::Poll::Pending => return std::task::Poll::Pending,
            std::task::Poll::Ready(waited) => {
                waiting = None;
                if waited.is_err() { return std::task::Poll::Ready(None); }
            }
        }
    }).await
}
