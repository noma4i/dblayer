use std::sync::Arc;

use super::life_tests::{closed, open, start};
use crate::runtime::{Epoch, Outcome, Waiter};
use crate::test_wait::within;
use crate::transport::MockTransport;

async fn ends_closed<T>(waiter: Waiter<T>) -> bool { matches!(within(waiter.wait()).await, Outcome::Closing) }

/// P-LIFE-23: once the owner threads are stopped no wait stays open: a wait open at the stop and a wait registered after it end with `Closing`, on every registry of waits.
#[tokio::test(flavor = "multi_thread")]
async fn life_prop_shutdown_ends_every_wait() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let transport = Arc::new(MockTransport::new());
    let db = open(&path);
    let life = Arc::new(start(&db, &transport).await);
    let wiring = &life.wiring;
    let before = (wiring.outcomes.register(Epoch::MAX), wiring.mutations.register(Epoch::MAX), wiring.calls_answers.register(Epoch::MAX), wiring.pulls.register(Epoch::MAX));
    closed(&life).await;
    let stopping = life.clone();
    tokio::task::spawn_blocking(move || stopping.shutdown()).await.unwrap();
    let after = (wiring.outcomes.register(Epoch::MAX), wiring.mutations.register(Epoch::MAX), wiring.calls_answers.register(Epoch::MAX), wiring.pulls.register(Epoch::MAX));
    assert!(ends_closed(before.0).await, "outcomes open at the stop");
    assert!(ends_closed(before.1).await, "mutations open at the stop");
    assert!(ends_closed(before.2).await, "calls_answers open at the stop");
    assert!(ends_closed(before.3).await, "pulls open at the stop");
    assert!(ends_closed(after.0).await, "outcomes after the stop");
    assert!(ends_closed(after.1).await, "mutations after the stop");
    assert!(ends_closed(after.2).await, "calls_answers after the stop");
    assert!(ends_closed(after.3).await, "pulls after the stop");
}
