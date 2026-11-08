use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use crate::ffi::tests::lifecycle_tests::{named, Rows};
use crate::ffi::tape::Watched;
use crate::ffi::{Database, ReadFetch, ReadRequest};
use crate::test_wait::settled;

/// The streams the app holds for the account it shows: the singleton and the id entity (CurrentUser), a list and a second list in another order.
/// A new account gets new streams on the same screens: `follow` opens them for the epoch now, the deliveries of every epoch stay in the same logs.
pub(super) struct Held {
    pub(super) single: Arc<Rows>,
    pub(super) entity: Arc<Rows>,
    pub(super) rows: Arc<Rows>,
    pub(super) window_rows: Arc<Rows>,
    open: Mutex<Vec<Arc<Watched>>>,
    stopped: std::sync::atomic::AtomicBool,
}

impl Held {
    pub(super) fn open(database: &Database) -> Held {
        let held = Held { single: Arc::default(), entity: Arc::default(), rows: Arc::default(), window_rows: Arc::default(), open: Mutex::new(Vec::new()), stopped: Default::default() };
        held.follow(database);
        held
    }

    /// Opens the streams for the epoch now. Streams of an epoch that has gone are cancelled first.
    pub(super) fn follow(&self, database: &Database) {
        let mut open = self.open.lock().unwrap();
        if open.first().is_some_and(|stream| stream.epoch == database.session_epoch()) { return; }
        self.stopped.store(false, Ordering::SeqCst);
        for stream in open.drain(..) { stream.cancel(); }
        open.push(database.watch(ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"User","relation":"profile","params":{}}"#) }, self.single.clone()));
        open.push(database.watch(ReadRequest::Entity { model: "User".into(), id: "user".into(), fetch: ReadFetch::Force }, self.entity.clone()));
        open.push(database.watch_query(r#"{"model":"User","relation":"all","params":{}}"#, self.rows.clone()));
        open.push(database.watch_query(r#"{"model":"User","relation":"all","params":{},"order":[{"field":"name","direction":"asc"}]}"#, self.window_rows.clone()));
    }

    /// The app cancels its collectors and lists before the data clear.
    pub(super) fn cancel_streams(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        for stream in self.open.lock().unwrap().drain(..) { stream.cancel(); }
    }

    pub(super) fn cancel_all(&self) { self.cancel_streams(); }

    /// The stream of the main list.
    pub(super) fn query(&self) -> Arc<Watched> { self.open.lock().unwrap()[2].clone() }

    /// The singleton and the id entity are over: their streams ended with their account or the reader stopped them.
    pub(super) fn entities_empty(&self) -> bool { self.stopped.load(Ordering::SeqCst) || [&self.single, &self.entity].iter().all(|rows| Self::over(rows)) }

    fn over(rows: &Rows) -> bool { !rows.ended().is_empty() || rows.len() == 0 }

    fn shown(rows: &Rows, account: &str) -> bool {
        let rows = rows.0.lock().unwrap();
        !rows.is_empty() && rows.iter().all(|row| named(row, account))
    }

    pub(super) async fn shows(&self, handle: &impl crate::test_wait::Quiet, account: &str) {
        settled(handle, &format!("the held entities show {account}"), || Self::shown(&self.single, account) && Self::shown(&self.entity, account)).await;
    }

    pub(super) async fn lists_show(&self, handle: &impl crate::test_wait::Quiet, account: &str) {
        for rows in [&self.rows, &self.window_rows] {
            settled(handle, &format!("the held list shows {account}"), || Self::shown(rows, account)).await;
        }
    }

    pub(super) fn deliveries(&self) -> (usize, usize, usize, usize) {
        let length = |rows: &Rows| rows.1.lock().unwrap().len();
        (length(&self.rows), length(&self.window_rows), length(&self.single), length(&self.entity))
    }

    fn showed(rows: &Rows, from: usize, account: &str) -> bool {
        rows.1.lock().unwrap().iter().skip(from).any(|seen| seen.rows.iter().any(|row| named(row, account)))
    }

    /// No delivery after `mark` shows the account.
    pub(super) fn quiet_since(&self, mark: (usize, usize, usize, usize), account: &str) -> bool {
        !Self::showed(&self.rows, mark.0, account) && !Self::showed(&self.window_rows, mark.1, account) && !Self::showed(&self.single, mark.2, account) && !Self::showed(&self.entity, mark.3, account)
    }

    /// The account never appears in a delivery of any held stream.
    pub(super) fn never_showed(&self, account: &str) -> bool {
        ![&self.rows, &self.window_rows, &self.single, &self.entity].iter().any(|rows| Self::showed(rows, 0, account))
    }
}
