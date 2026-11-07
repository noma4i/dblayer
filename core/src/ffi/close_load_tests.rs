use std::sync::Arc;

use super::close_tests::with_transport;
use super::{ReadRequest, ReadValue};
use crate::runtime::{Clock, SystemClock};
use crate::test_wait::within_secs;

/// P-LIFE-22: `close` returns for every database while loads, streams and a network subscription are in flight, on a loaded multi-thread runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn life_prop_close_under_load() {
    let lanes: Vec<_> = (0..16).map(|lane| tokio::spawn(async move {
        for cycle in 0..25 {
            let clock: Arc<dyn Clock> = Arc::new(SystemClock);
            let (database, _transport) = with_transport(clock).await;
            let epoch = database.session_epoch();
            let streams: Vec<_> = (0..3).map(|_| database.subscribe_values(epoch, ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Notice"}"#) })).collect();
            let refreshing: Vec<_> = streams.iter().cloned().map(|stream| tokio::spawn(async move { let _ = stream.refresh().await; })).collect();
            let pulling: Vec<_> = streams.iter().cloned().map(|stream| tokio::spawn(async move { while let Some(value) = stream.pull().await { if matches!(value, ReadValue::Ended { .. }) { break; } } })).collect();
            within_secs(15, database.close_database()).await.unwrap_or_else(|error| panic!("lane {lane} cycle {cycle}: {error:?}"));
            for task in refreshing.into_iter().chain(pulling) { within_secs(15, task).await.unwrap(); }
        }
    })).collect();
    for lane in lanes { within_secs(18, lane).await.unwrap(); }
}
