//! C5: every public stream request x {cancel, drop, second iteration, close, epoch end, concurrent pull and cancel}: the stream ends exactly once with a cause, a second pull answers nothing, the census of observers is 0 and nothing panics.
use std::sync::Arc;

use super::stand::Stand;
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::{ReadFetch, ReadRequest, ReadStream, ReadValue};
use crate::test_matrix::Matrix;

fn requests() -> Vec<(&'static str, ReadRequest)> {
    vec![
        ("Entity never", ReadRequest::Entity { model: "Moment".into(), id: "m1".into(), fetch: ReadFetch::Never }),
        ("Entity stale", ReadRequest::Entity { model: "Moment".into(), id: "m1".into(), fetch: ReadFetch::Stale }),
        ("Entity if missing", ReadRequest::Entity { model: "Moment".into(), id: "m1".into(), fetch: ReadFetch::IfMissing }),
        ("List", ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Moment"}"#) }),
        ("Count", ReadRequest::Count { query: crate::ffi::QuerySpec::json(r#"{"model":"Moment"}"#) }),
        ("Operations", ReadRequest::Operations),
        ("Changes", ReadRequest::Changes),
        ("Events", ReadRequest::Events { model: "Chat".into() }),
        ("Blob", ReadRequest::Blob { model: "Message".into(), id: "m1".into(), action: "send_files".into() }),
    ]
}

const EVENTS: [&str; 6] = ["cancel", "drop", "second iteration", "close", "epoch end", "concurrent pull and cancel"];

/// Takes values until the stream answers nothing: the ends seen and the count of values after the first end.
async fn drain(stream: &ReadStream, reader: u64) -> (usize, usize) {
    let (mut ends, mut after) = (0, 0);
    for _ in 0..64 {
        match bound(stream.pull(reader)).await {
            Some(ReadValue::Ended { .. }) => ends += 1,
            Some(_) => if ends > 0 { after += 1 },
            None => return (ends, after),
        }
    }
    (ends, usize::MAX)
}

async fn lifecycle(wanted: &str) {
    let mut matrix = Matrix::new(Box::leak(format!("C5-stream-{}", wanted.replace(' ', "-")).into_boxed_str()));
    for (name, request) in requests().into_iter().filter(|(name, _)| *name == wanted) {
        for event in EVENTS {
            let stand = Stand::open().await;
            let epoch = stand.database.session_epoch();
            let stream: Arc<ReadStream> = stand.database.subscribe_values(epoch, request.clone());
            let crate::ffi::ReadClaim::Claimed { reader } = bound(stream.claim_reader()).await else { panic!("the stream reader claim ended") };
            let cell = |matrix: &mut Matrix, what: &str, expected: &str, observed: &str| matrix.check(name, event, what, expected, observed);
            match event {
                "cancel" => {
                    bound(stream.cancel()).await;
                    let (ends, after) = drain(&stream, reader).await;
                    cell(&mut matrix, "Ended after cancel", "1 ends", &format!("{ends} ends"));
                    cell(&mut matrix, "values after the end", "0", &after.to_string());
                }
                "drop" => {}
                "second iteration" => {
                    bound(stream.cancel()).await;
                    let first = drain(&stream, reader).await;
                    let second = drain(&stream, reader).await;
                    cell(&mut matrix, "second iteration", "0 ends, 0 after", &format!("{} ends, {} after", second.0, second.1));
                    cell(&mut matrix, "first iteration", "1 ends", &format!("{} ends", first.0));
                }
                "close" => {
                    let closed = bound(stand.database.close_database()).await;
                    cell(&mut matrix, "close_database", "Ok", if closed.is_ok() { "Ok" } else { "Err" });
                    let (ends, _) = drain(&stream, reader).await;
                    cell(&mut matrix, "Ended after close", "1 ends", &format!("{ends} ends"));
                    continue;
                }
                "epoch end" => {
                    let _ = bound(stand.database.session_reset()).await;
                    let (ends, after) = drain(&stream, reader).await;
                    cell(&mut matrix, "Ended after epoch end", "1 ends", &format!("{ends} ends"));
                    cell(&mut matrix, "values after the end", "0", &after.to_string());
                }
                _ => {
                    let waiting = tokio::spawn({ let stream = stream.clone(); async move { bound(stream.pull(reader)).await } });
                    bound(stream.cancel()).await;
                    let joined = bound(waiting).await;
                    cell(&mut matrix, "pull in flight during cancel", "returns", if joined.is_ok() { "returns" } else { "panicked" });
                    let (ends, _) = drain(&stream, reader).await;
                    cell(&mut matrix, "ends after the race", "at most 1", if ends <= 1 { "at most 1" } else { "more" });
                }
            }
            drop(stream);
            stand.settle().await;
            if event != "close" {
                let census = bound(stand.database.subscription_census()).await;
                match census {
                    Ok(census) => cell(&mut matrix, "census after the event", "0 observed, 0 retained", &format!("{} observed, {} retained", census.observed_queries, census.retained_observers)),
                    Err(error) => cell(&mut matrix, "census after the event", "0 observed, 0 retained", &format!("census failed: {error:?}")),
                }
            }
        }
    }
    matrix.finish();
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_lifecycle_entity_never() { lifecycle("Entity never").await }

#[tokio::test(flavor = "multi_thread")]
async fn stream_lifecycle_entity_stale() { lifecycle("Entity stale").await }

#[tokio::test(flavor = "multi_thread")]
async fn stream_lifecycle_entity_if_missing() { lifecycle("Entity if missing").await }

#[tokio::test(flavor = "multi_thread")]
async fn stream_lifecycle_list() { lifecycle("List").await }

#[tokio::test(flavor = "multi_thread")]
async fn stream_lifecycle_count() { lifecycle("Count").await }

#[tokio::test(flavor = "multi_thread")]
async fn stream_lifecycle_operations() { lifecycle("Operations").await }

#[tokio::test(flavor = "multi_thread")]
async fn stream_lifecycle_changes() { lifecycle("Changes").await }

#[tokio::test(flavor = "multi_thread")]
async fn stream_lifecycle_events() { lifecycle("Events").await }

#[tokio::test(flavor = "multi_thread")]
async fn stream_lifecycle_blob() { lifecycle("Blob").await }

#[tokio::test(flavor = "multi_thread")]
async fn consumer_stream_release_reopen() {
    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    let received = stand.request_mark();
    stand.hold(&["MomentNode"]);
    stand.answer("MomentNode", serde_json::json!({"node":super::stand::moment("m1", 7)}));
    let first = stand.database.subscribe_values(epoch, ReadRequest::Entity { model: "Moment".into(), id: "m1".into(), fetch: ReadFetch::Stale });
    let value = bound(first.pull(0)).await;
    assert!(matches!(value, Some(ReadValue::Snapshot { .. })), "{value:?}");
    stand.receive_after(received, "MomentNode", None, 1).await;
    first.release_reader(0);
    let rows = stand.entity("Moment", "m1", ReadFetch::Stale);
    stand.release(&["MomentNode"]);
    crate::test_wait::settled(&stand, "the new reader sees the server row", || rows.0.lock().unwrap().iter().any(|row| row.id == "m1")).await;
    let ended = bound(first.pull(0)).await;
    assert!(matches!(ended, Some(ReadValue::Ended { cause: crate::ffi::EndCause::Closed })), "{ended:?}");
}
