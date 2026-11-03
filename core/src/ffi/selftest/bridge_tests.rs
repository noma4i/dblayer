use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;

use serde_json::json;

use super::rig_tests::{bound, Rig};
use crate::ffi::lifecycle_tests::named;
use crate::ffi::{CommandRequest, CommandStatus, DbError, Placement, Replacement, StreamDelivery, StreamEnd, StreamRequest, StreamValue, ValueSink, WireRow, WireSyncState};
use crate::test_wait::yield_until;

/// What a platform reader holds: the deliveries in order, replayed into the rows its screen shows.
#[derive(Default)]
struct Reader {
    log: Mutex<Vec<StreamDelivery>>,
    threads: Mutex<Vec<ThreadId>>,
    lose_next: AtomicBool,
}

impl ValueSink for Reader {
    fn delivered(&self, delivery: StreamDelivery) {
        self.threads.lock().unwrap().push(std::thread::current().id());
        if self.lose_next.swap(false, Ordering::SeqCst) { return; }
        self.log.lock().unwrap().push(delivery);
    }
}

fn apply_delta(rows: &mut Vec<WireRow>, carried: Vec<WireRow>, indexes: &[u32], removed: &[String], placed: &[Placement], replaced: &[Replacement], size: u32) {
    let by_id: HashMap<String, WireRow> = carried.iter().map(|row| (row.id.clone(), row.clone())).collect();
    for pair in replaced {
        let at = rows.iter().position(|row| row.id == pair.from).expect("a replaced row is held");
        rows[at] = by_id[&pair.to].clone();
    }
    rows.retain(|row| !removed.contains(&row.id));
    let mut taken = HashMap::new();
    for place in placed {
        if let Some(at) = rows.iter().position(|row| row.id == place.id) { taken.insert(place.id.clone(), rows.remove(at)); }
    }
    let mut ordered: Vec<&Placement> = placed.iter().collect();
    ordered.sort_by_key(|place| place.index);
    for place in ordered {
        let row = taken.remove(&place.id).or_else(|| by_id.get(&place.id).cloned()).expect("a placed row is known");
        rows.insert(place.index as usize, row);
    }
    for (row, index) in carried.into_iter().zip(indexes) {
        let index = *index as usize;
        if rows[index].id == row.id && rows[index].cells == row.cells && rows[index].sync == row.sync { continue; }
        rows[index] = row;
    }
    assert_eq!(rows.len(), size as usize, "the delta leaves the size it names");
}

impl Reader {
    fn open() -> Arc<Reader> { Arc::new(Reader::default()) }
    fn deliveries(&self) -> Vec<StreamDelivery> { self.log.lock().unwrap().clone() }
    fn len(&self) -> usize { self.log.lock().unwrap().len() }

    /// The rows after the last snapshot and the deltas that chain from it. A delta whose base is not the held revision is a gap and is counted.
    fn replay(&self) -> (Vec<WireRow>, usize) {
        let (mut rows, mut revision, mut gaps) = (Vec::new(), None, 0);
        for delivery in self.deliveries() {
            match delivery.value {
                StreamValue::Snapshot { revision: held, rows: whole, .. } => { rows = whole; revision = Some(held); }
                StreamValue::Delta { base, revision: next, size, rows: carried, row_indexes, removed, placed, replaced, .. } => {
                    if revision != Some(base) { gaps += 1; continue; }
                    apply_delta(&mut rows, carried, &row_indexes, &removed, &placed, &replaced, size);
                    revision = Some(next);
                }
                StreamValue::State { .. } | StreamValue::Ended { .. } => {}
            }
        }
        (rows, gaps)
    }

    fn rows(&self) -> Vec<WireRow> { self.replay().0 }
    fn gaps(&self) -> usize { self.replay().1 }
    fn shows(&self, account: &str) -> bool { let rows = self.rows(); !rows.is_empty() && rows.iter().all(|row| named(row, account)) }
    fn ever_showed(&self, account: &str) -> bool {
        self.deliveries().iter().any(|delivery| match &delivery.value {
            StreamValue::Snapshot { rows, .. } | StreamValue::Delta { rows, .. } => rows.iter().any(|row| named(row, account)),
            _ => false,
        })
    }
    fn ended(&self) -> Vec<StreamEnd> {
        self.deliveries().into_iter().filter_map(|delivery| if let StreamValue::Ended { reason } = delivery.value { Some(reason) } else { None }).collect()
    }
    fn snapshots(&self) -> usize { self.deliveries().iter().filter(|delivery| matches!(delivery.value, StreamValue::Snapshot { .. })).count() }
    fn texts(&self) -> Vec<String> {
        self.rows().iter().map(|row| row.cells.iter().filter_map(|cell| if let crate::ffi::Cell::Text { value } = cell { Some(value.clone()) } else { None }).collect::<Vec<_>>().join("|")).collect()
    }
}

fn profile() -> StreamRequest { StreamRequest::Query { query_json: r#"{"model":"User","relation":"profile","params":{}}"#.into() } }

fn users() -> StreamRequest { StreamRequest::Query { query_json: r#"{"model":"User","relation":"all","params":{}}"#.into() } }

fn thread() -> StreamRequest {
    StreamRequest::Query { query_json: r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"},"order":[{"field":"at","direction":"asc"}]}"#.into() }
}

fn message(epoch: u64, text: &str, at: i64) -> CommandRequest {
    CommandRequest { epoch, model: "Message".into(), values_json: json!({"chatId":"c1","text":text,"at":at}).to_string(), args_json: "{}".into() }
}

async fn settle(rig: &Rig) { rig.settle().await; }

/// B01: after the login a stream delivers the account as one snapshot in the epoch of the session, in order, and the first value does not come from the calling thread.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_opened_after_the_login_starts_with_a_snapshot_of_the_account_in_its_epoch() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (reader, caller) = (Reader::open(), std::thread::current().id());
    let stream = rig.database.subscribe(epoch, profile(), reader.clone());
    bound(rig.database.stream_loaded(stream)).await.unwrap();
    yield_until("the stream shows A", || reader.shows("A")).await;
    let log = reader.deliveries();
    assert!(matches!(log[0].value, StreamValue::Snapshot { .. }), "the first value is a snapshot: {:?}", log[0]);
    assert!(log.iter().all(|delivery| delivery.epoch == epoch && delivery.stream == stream), "every delivery names the stream and its epoch");
    assert!(log.iter().enumerate().all(|(index, delivery)| delivery.seq == index as u64 + 1), "the sequence counts from 1 with no gap");
    assert!(reader.threads.lock().unwrap().iter().all(|thread| *thread != caller), "a delivery ran on the thread that subscribed");
    rig.database.close_now();
}

/// B02: the sign-out ends the stream with one outcome and no value of the account follows. Nothing stays open in the core.
#[tokio::test(flavor = "multi_thread")]
async fn the_session_reset_ends_every_stream_with_one_outcome_and_no_account_value_follows() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (profile_log, users_log) = (Reader::open(), Reader::open());
    let streams = [rig.database.subscribe(epoch, profile(), profile_log.clone()), rig.database.subscribe(epoch, users(), users_log.clone())];
    for stream in streams { bound(rig.database.stream_loaded(stream)).await.unwrap(); }
    yield_until("both streams show A", || profile_log.shows("A") && users_log.shows("A")).await;

    bound(rig.database.session_reset()).await.unwrap();
    settle(&rig).await;

    for log in [&profile_log, &users_log] {
        assert_eq!(log.ended(), vec![StreamEnd::SessionEnded], "exactly one outcome");
        assert!(matches!(log.deliveries().last().unwrap().value, StreamValue::Ended { .. }), "nothing follows the outcome");
    }
    assert_eq!(rig.database.stream_census().open, 0);
    assert_eq!(rig.database.subscription_census().subscriptions, 0);
    let marks = (profile_log.len(), users_log.len());
    rig.world.forbid(true);
    settle(&rig).await;
    assert!(rig.world.violations().is_empty(), "a read followed the exit: {:?}", rig.world.violations());
    assert_eq!(marks, (profile_log.len(), users_log.len()));
    rig.database.close_now();
}

/// B03: the account switch A -> B gives new streams in a new epoch. The old projection ends, B never shows A and A never shows B.
#[tokio::test(flavor = "multi_thread")]
async fn an_account_switch_gives_new_streams_and_no_value_of_a_reaches_b() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch_a = rig.database.session_epoch();
    let (first_profile, first_users) = (Reader::open(), Reader::open());
    for (request, log) in [(profile(), &first_profile), (users(), &first_users)] {
        let stream = rig.database.subscribe(epoch_a, request, log.clone());
        bound(rig.database.stream_loaded(stream)).await.unwrap();
    }
    yield_until("A shows", || first_profile.shows("A") && first_users.shows("A")).await;

    bound(rig.database.session_reset()).await.unwrap();
    rig.login_ios("B").await;
    let epoch_b = rig.database.session_epoch();
    assert_ne!(epoch_a, epoch_b, "the switch moved the epoch");
    let (second_profile, second_users) = (Reader::open(), Reader::open());
    for (request, log) in [(profile(), &second_profile), (users(), &second_users)] {
        let stream = rig.database.subscribe(epoch_b, request, log.clone());
        bound(rig.database.stream_loaded(stream)).await.unwrap();
    }
    yield_until("B shows", || second_profile.shows("B") && second_users.shows("B")).await;
    settle(&rig).await;

    assert!(!second_profile.ever_showed("A") && !second_users.ever_showed("A"), "a value of A reached the projections of B");
    assert!(!first_profile.ever_showed("B") && !first_users.ever_showed("B"), "a value of B reached the projections of A");
    assert!(second_profile.deliveries().iter().all(|delivery| delivery.epoch == epoch_b));
    assert_eq!(first_profile.ended(), vec![StreamEnd::SessionEnded]);
    rig.database.close_now();
}

/// B04: a message sent offline shows at once, goes out once when the network returns, and keeps its position when the server confirms it.
#[tokio::test(flavor = "multi_thread")]
async fn a_message_sent_offline_goes_out_once_and_keeps_its_position() {
    let rig = Rig::encrypted().await;
    rig.world.seed_message("A", "m1", "first", 1);
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    let stream = rig.database.subscribe(epoch, thread(), reader.clone());
    bound(rig.database.stream_loaded(stream)).await.unwrap();
    yield_until("the thread shows the first message", || reader.texts().iter().any(|text| text.contains("first"))).await;

    rig.database.set_online(false).unwrap();
    let receipt = bound(rig.database.send(message(epoch, "second", 2))).await.unwrap();
    yield_until("the thread shows the queued message", || reader.rows().len() == 2).await;
    let queued = reader.rows();
    assert_eq!(queued[1].id, receipt.row.id, "the new message sits after the first");
    assert_eq!(queued[1].sync.state, WireSyncState::Pending);
    assert_eq!(rig.database.command_status(receipt.clone()).unwrap(), CommandStatus::Queued);
    assert_eq!(rig.world.count("SendMessage"), 0, "the offline send reached the server");

    rig.database.set_online(true).unwrap();
    let outcome = bound(rig.database.command_outcome(receipt.clone())).await.unwrap();
    assert_eq!(outcome, CommandStatus::Delivered { id: "m2".into() });
    yield_until("the thread shows the confirmed message", || reader.rows().get(1).is_some_and(|row| row.id == "m2")).await;
    settle(&rig).await;

    assert_eq!(rig.world.count("SendMessage"), 1, "the message went out once");
    let confirmed = reader.rows();
    assert_eq!(confirmed.len(), 2, "one message, not a temporary row and a confirmed row");
    assert_eq!((confirmed[0].id.as_str(), confirmed[1].id.as_str()), ("m1", "m2"), "the position did not change");
    assert_eq!(confirmed[1].sync.state, WireSyncState::Synced);
    assert_eq!(reader.gaps(), 0);
    rig.database.close_now();
}

/// B05: an answer of the old epoch that arrives late changes nothing: the call ends as interrupted, the new projection never shows it, and a command of the old epoch is refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_late_answer_of_the_old_epoch_is_dropped_and_a_command_of_the_old_epoch_is_refused() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch_a = rig.database.session_epoch();
    let first = Reader::open();
    let stream = rig.database.subscribe(epoch_a, users(), first.clone());
    bound(rig.database.stream_loaded(stream)).await.unwrap();
    yield_until("A shows", || first.shows("A")).await;

    let gate = rig.world.hold_next("Users").as_account("A");
    let late = { let database = rig.database.clone(); tokio::spawn(async move { database.refresh_stream(stream).await }) };
    gate.entered().await;
    bound(rig.database.session_reset()).await.unwrap();
    rig.login_ios("B").await;
    let epoch_b = rig.database.session_epoch();
    let second = Reader::open();
    let next = rig.database.subscribe(epoch_b, users(), second.clone());
    bound(rig.database.stream_loaded(next)).await.unwrap();
    yield_until("B shows", || second.shows("B")).await;
    gate.release();

    let answer = bound(late).await.unwrap();
    assert!(matches!(answer, Err(DbError::Interrupted { .. })), "the late call of the old epoch ended as {answer:?}");
    settle(&rig).await;
    assert!(!second.ever_showed("A"), "the late answer of A reached the projection of B");
    assert!(second.shows("B"));
    let refused = bound(rig.database.send(message(epoch_a, "late", 3))).await;
    assert!(matches!(refused, Err(DbError::Interrupted { .. })), "a command of the old epoch ran: {refused:?}");
    assert_eq!(rig.world.count("SendMessage"), 0);
    rig.database.close_now();
}

/// B06: the reader that cancels ends the stream in the core: no subscription stays and no value is delivered after.
#[tokio::test(flavor = "multi_thread")]
async fn the_reader_that_cancels_ends_the_stream_and_the_core_keeps_nothing() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    let stream = rig.database.subscribe(epoch, users(), reader.clone());
    bound(rig.database.stream_loaded(stream)).await.unwrap();
    yield_until("A shows", || reader.shows("A")).await;
    assert_eq!(rig.database.stream_census().open, 1);

    rig.database.cancel_stream(stream);

    assert_eq!(rig.database.stream_census().open, 0);
    assert_eq!(rig.database.subscription_census().subscriptions, 0);
    assert!(reader.ended().is_empty(), "a cancelled stream delivers no outcome");
    let mark = reader.len();
    rig.world.set_account("A2");
    bound(rig.database.refresh("User".into(), "all".into(), "{}".into())).await.unwrap();
    settle(&rig).await;
    assert_eq!(reader.len(), mark, "a value reached a cancelled reader");
    assert!(matches!(bound(rig.database.refresh_stream(stream)).await, Err(DbError::Interrupted { .. })));
    rig.database.close_now();
}

/// B07, spec F: an idle stream costs no request and no wakeup, a change of one row delivers one row, and the number of subscriptions is the number of readers.
#[tokio::test(flavor = "multi_thread")]
async fn work_counters_idle_zero_one_row_one_decode_and_subscriptions_equal_readers() {
    let rig = Rig::encrypted().await;
    for (at, text) in [(1, "one"), (2, "two"), (3, "three"), (4, "four"), (5, "five")] { rig.world.seed_message("A", &format!("s{at}"), text, at); }
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (reader, other) = (Reader::open(), Reader::open());
    let stream = rig.database.subscribe(epoch, thread(), reader.clone());
    let second = rig.database.subscribe(epoch, profile(), other.clone());
    bound(rig.database.stream_loaded(stream)).await.unwrap();
    bound(rig.database.stream_loaded(second)).await.unwrap();
    yield_until("the thread holds 5 rows", || reader.rows().len() == 5).await;
    settle(&rig).await;
    assert_eq!(rig.database.stream_census().open, 2);
    assert_eq!(rig.database.subscription_census().subscriptions, 2, "subscriptions are readers");

    let idle = (rig.world.hits(), rig.database.db.counters().observer_calls, reader.len(), other.len());
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    assert_eq!(idle, (rig.world.hits(), rig.database.db.counters().observer_calls, reader.len(), other.len()), "an idle stream did work");

    let mut costs = Vec::new();
    for (index, text) in ["six", "seven"].into_iter().enumerate() {
        let (before, mark) = (rig.database.db.counters().rows_delivered, reader.len());
        let receipt = bound(rig.database.send(message(epoch, text, 6 + index as i64))).await.unwrap();
        bound(rig.database.command_outcome(receipt)).await.unwrap();
        settle(&rig).await;
        let delta_rows: Vec<usize> = reader.deliveries().iter().skip(mark).filter_map(|delivery| if let StreamValue::Delta { rows, .. } = &delivery.value { Some(rows.len()) } else { None }).collect();
        assert!(!delta_rows.is_empty() && delta_rows.iter().all(|rows| *rows == 1), "a change of one row carried {delta_rows:?} rows");
        costs.push((delta_rows.len(), rig.database.db.counters().rows_delivered - before));
    }
    assert_eq!(costs[0], costs[1], "the cost of one change grew with the list: {costs:?}");
    assert!(costs[1].1 <= 2 * costs[1].0, "the core delivered more rows than the deltas carry: {costs:?}");
    rig.database.close_now();
}

/// A missed revision gives exactly one new snapshot, never a replay of the history.
#[tokio::test(flavor = "multi_thread")]
async fn a_missed_revision_gives_one_new_snapshot() {
    let rig = Rig::encrypted().await;
    rig.world.seed_message("A", "m1", "first", 1);
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    let stream = rig.database.subscribe(epoch, thread(), reader.clone());
    bound(rig.database.stream_loaded(stream)).await.unwrap();
    yield_until("the thread shows the first message", || reader.rows().len() == 1).await;
    let snapshots = reader.snapshots();

    reader.lose_next.store(true, Ordering::SeqCst);
    bound(rig.database.send(message(epoch, "lost", 2))).await.unwrap();
    yield_until("the lost delivery was swallowed", || !reader.lose_next.load(Ordering::SeqCst)).await;
    bound(rig.database.send(message(epoch, "after", 3))).await.unwrap();
    yield_until("the delta after the gap arrived", || reader.gaps() == 1).await;

    rig.database.resync_stream(stream);
    yield_until("one new snapshot arrived", || reader.snapshots() == snapshots + 1).await;
    yield_until("the rows are whole again", || reader.rows().len() == 3).await;
    settle(&rig).await;
    assert_eq!(reader.snapshots(), snapshots + 1, "more than one snapshot answered the gap");
    rig.database.close_now();
}
