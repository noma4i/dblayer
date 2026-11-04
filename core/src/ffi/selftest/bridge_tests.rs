use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;

use serde_json::json;

use super::rig_tests::{bound, Fault, Rig};
use crate::ffi::lifecycle_tests::named;
use crate::ffi::{Database, CommandRequest, CommandStatus, DbError, Placement, Replacement, StreamDelivery, StreamEnd, StreamRequest, StreamValue, ValueSink, WireRow, WireSyncState};
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
                _ => {}
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

pub(super) fn message(epoch: u64, text: &str, at: i64) -> CommandRequest {
    CommandRequest { epoch, model: "Message".into(), values_json: json!({"chatId":"c1","text":text,"at":at}).to_string(), args_json: "{}".into(), upload: None }
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
    let newest = bound(rig.database.stream_loaded(stream)).await.unwrap();
    yield_until("the stream shows A", || reader.shows("A")).await;
    let log = reader.deliveries();
    assert!(log.iter().any(|delivery| delivery.seq == newest), "loaded names {newest}, a sequence number the reader did not receive: {log:?}");
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
    bound(rig.database.refresh(rig.database.session_epoch(), "User".into(), "all".into(), "{}".into())).await.unwrap();
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

/// B15, spec F: a write to one row of a list delivers nothing to the entity stream of another row, and one value to a count stream, however long the list is.
#[tokio::test(flavor = "multi_thread")]
async fn work_counters_an_entity_stream_ignores_other_rows_and_a_count_stream_gets_one_value_per_write() {
    let rig = Rig::encrypted().await;
    for (at, text) in [(1, "one"), (2, "two"), (3, "three"), (4, "four"), (5, "five")] { rig.world.seed_message("A", &format!("s{at}"), text, at); }
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (entity, count, list) = (Reader::open(), Reader::open(), Reader::open());
    let streams = [
        rig.database.subscribe(epoch, StreamRequest::Entity { model: "Message".into(), id: "s1".into(), fetch: crate::ffi::EntityFetch::None }, entity.clone()),
        rig.database.subscribe(epoch, StreamRequest::Count { query_json: r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"}}"#.into() }, count.clone()),
        rig.database.subscribe(epoch, thread(), list.clone()),
    ];
    for stream in streams { bound(rig.database.stream_loaded(stream)).await.unwrap(); }
    yield_until("the list holds 5 rows", || list.rows().len() == 5).await;
    settle(&rig).await;
    assert_eq!(rig.database.subscription_census().subscriptions, 3, "subscriptions are readers");

    let mut costs = Vec::new();
    for (index, text) in ["six", "seven"].into_iter().enumerate() {
        let (entity_mark, count_mark) = (entity.len(), count.len());
        let receipt = bound(rig.database.send(message(epoch, text, 6 + index as i64))).await.unwrap();
        bound(rig.database.command_outcome(receipt)).await.unwrap();
        settle(&rig).await;
        let entity_values = entity.deliveries().len() - entity_mark;
        let counts: Vec<i64> = count.deliveries().iter().skip(count_mark).filter_map(|delivery| if let StreamValue::Count { count, .. } = &delivery.value { Some(*count) } else { None }).collect();
        assert_eq!(entity_values, 0, "a write to another row reached the entity stream");
        assert_eq!(counts.last().copied(), Some(6 + index as i64), "the count stream shows {counts:?}");
        costs.push(counts.len());
    }
    assert_eq!(costs[0], costs[1], "the cost of one write to the count stream grew with the list: {costs:?}");
    assert!(costs[1] <= 2, "one write delivered {} count values", costs[1]);
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

/// B09 (R1, old line command.rs:46-55): a send that waits for the writer while the session ends writes no row. The commit takes the epoch of the caller, not the generation of the data, which moves later.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::await_holding_lock)]
async fn a_send_that_waits_for_the_writer_while_the_session_ends_writes_no_row() {
    use crate::lifecycle::{Boundary, End};
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    rig.database.set_online(false).unwrap();
    let store = &rig.database.db.shared.store;
    let pinned = store.lifecycle.writes_entered.load(Ordering::SeqCst);
    let writer = store.lock();
    let sender = { let database = rig.database.clone(); tokio::spawn(async move { database.send(message(epoch, "late", 9)).await }) };
    yield_until("the send reached the writer", || store.lifecycle.writes_entered.load(Ordering::SeqCst) > pinned).await;
    let permit = store.lifecycle.try_begin(Boundary::SessionEnd(End::Local), Some(epoch)).ok().and_then(Result::ok).expect("the epoch of the caller is current");
    drop(writer);
    let sent = bound(sender).await.unwrap();
    assert!(matches!(sent, Err(DbError::Interrupted { .. })), "a send of the ended session ran: {sent:?}");
    yield_until("the admitted write finished", || store.lifecycle.writes_entered.load(Ordering::SeqCst) == pinned).await;
    assert_eq!(store.row_count("Message").unwrap(), 0, "the send wrote its row after its session ended");
    drop(permit);
    rig.database.close_now();
}

/// B10 (R2, old line command.rs:76-85): the outcome waits while the command is queued offline and is owned by the epoch of its receipt: it ends with the session, and a receipt of an old epoch stays ended.
#[tokio::test(flavor = "multi_thread")]
async fn the_outcome_of_an_offline_command_waits_and_ends_with_the_epoch_of_its_receipt() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    rig.database.set_online(false).unwrap();
    let receipt = bound(rig.database.send(message(epoch, "queued", 2))).await.unwrap();
    let outcome = { let database = rig.database.clone(); let receipt = receipt.clone(); tokio::spawn(async move { database.command_outcome(receipt).await }) };
    for _ in 0..50 { tokio::task::yield_now().await; }
    assert!(!outcome.is_finished(), "the outcome of a queued command returned before the command settled");
    bound(rig.database.session_reset()).await.unwrap();
    assert_eq!(bound(outcome).await.unwrap().unwrap(), CommandStatus::Ended);
    rig.database.set_online(true).unwrap();
    rig.login_ios("B").await;
    assert_eq!(rig.database.command_status(receipt.clone()).unwrap(), CommandStatus::Ended, "a receipt of A answered in the epoch of B");
    assert_eq!(bound(rig.database.command_outcome(receipt)).await.unwrap(), CommandStatus::Ended);
    assert_eq!(rig.world.count("SendMessage"), 0);
    rig.database.close_now();
}

/// B11 (R3, old line stream/mod.rs refresh_stream): a stream request whose caller left does not outlive its epoch: the transition does not wait for the request.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_request_whose_caller_left_ends_with_its_epoch_and_holds_nothing() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    let stream = rig.database.subscribe(epoch, users(), reader.clone());
    bound(rig.database.stream_loaded(stream)).await.unwrap();
    let gate = rig.world.hold_next("Users").as_account("A");
    let refresh = { let database = rig.database.clone(); tokio::spawn(async move { database.refresh_stream(stream).await }) };
    gate.entered().await;
    refresh.abort();
    let _ = refresh.await;
    bound(rig.database.session_reset()).await.unwrap();
    rig.login_ios("B").await;
    gate.release();
    settle(&rig).await;
    assert!(!rig.database.streams.held().iter().any(|held| held.contains("stream")), "a request of the left caller holds a lease: {:?}", rig.database.streams.held());
    rig.database.close_now();
}

/// B12 (R4, old line stream/open.rs connect): a stream whose source cannot be connected ends with one outcome and leaves nothing in the table.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_that_cannot_connect_ends_once_and_leaves_the_table() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let requests = [
        StreamRequest::Entity { model: "Nope".into(), id: "x".into(), fetch: crate::ffi::EntityFetch::None },
        StreamRequest::Events { model: "Nope".into() },
        StreamRequest::Count { query_json: r#"{"model":"Nope"}"#.into() },
    ];
    for request in requests {
        let reader = Reader::open();
        let stream = rig.database.subscribe(epoch, request.clone(), reader.clone());
        yield_until("the failed stream ended", || !reader.ended().is_empty()).await;
        assert_eq!(reader.ended(), vec![StreamEnd::Invalid], "{request:?} ended with more or less than one outcome");
        assert_eq!(rig.database.stream_census().open, 0, "{request:?} stayed in the table");
        assert!(bound(rig.database.stream_loaded(stream)).await.is_err(), "{request:?} reported a first load");
    }
    rig.database.close_now();
}

/// A reader whose first delivery after arming waits for a release.
struct Blocking {
    inner: Arc<Reader>,
    armed: AtomicBool,
    finished: AtomicBool,
    entered: Mutex<std::sync::mpsc::Sender<()>>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl ValueSink for Blocking {
    fn delivered(&self, delivery: StreamDelivery) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.lock().unwrap().send(()).unwrap();
            let _ = self.release.lock().unwrap().recv_timeout(std::time::Duration::from_secs(20));
            self.inner.delivered(delivery);
            self.finished.store(true, Ordering::SeqCst);
            return;
        }
        self.inner.delivered(delivery);
    }
}

/// B13 (R5, B5; old line stream/owner.rs cancelled): `cancel_stream` returns while a delivery in the reader is still running, and no delivery begins after it returns.
#[tokio::test(flavor = "multi_thread")]
async fn the_cancel_of_a_reader_returns_while_a_delivery_runs_and_none_is_admitted_after_it() {
    let rig = Rig::encrypted().await;
    rig.world.seed_message("A", "m1", "first", 1);
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (entered, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release) = std::sync::mpsc::channel();
    let blocking = Arc::new(Blocking { inner: Reader::open(), armed: AtomicBool::new(false), finished: AtomicBool::new(false), entered: Mutex::new(entered), release: Mutex::new(release) });
    let stream = rig.database.subscribe(epoch, thread(), blocking.clone());
    bound(rig.database.stream_loaded(stream)).await.unwrap();
    yield_until("the thread shows the first message", || blocking.inner.texts().iter().any(|text| text.contains("first"))).await;
    rig.database.set_online(false).unwrap();
    blocking.armed.store(true, Ordering::SeqCst);
    bound(rig.database.send(message(epoch, "second", 2))).await.unwrap();
    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap()).await.unwrap();
    rig.database.cancel_stream(stream);
    assert!(!blocking.finished.load(Ordering::SeqCst), "the cancel waited for the reader to finish a delivery");
    release_tx.send(()).unwrap();
    yield_until("the delivery in progress finished", || blocking.finished.load(Ordering::SeqCst)).await;
    let after_cancel = blocking.inner.len();
    bound(rig.database.send(message(epoch, "third", 3))).await.unwrap();
    rig.settle().await;
    assert_eq!(blocking.inner.len(), after_cancel, "a delivery began after the cancel returned");
    assert!(!blocking.inner.texts().iter().any(|text| text.contains("third")));
    rig.database.close_now();
}

pub(super) fn group(name: &str) -> (String, String) { (json!({"name": name}).to_string(), "{}".into()) }

/// K01 (B1, J02): a send that passed its epoch check and waits before its SQL commit lands before the next sign-in changes the epoch, and its announcement is held until the session start finished: the operation of the account that left is in the cutoff and never goes out for the account that came.
#[tokio::test(flavor = "multi_thread")]
async fn a_send_between_its_epoch_check_and_its_commit_never_goes_out_for_the_next_account() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    rig.database.set_online(false).unwrap();
    let store = &rig.database.db.shared.store;
    let operations_begun = |rig: &Rig| rig.database.db.shared.read(&[], |reader| Ok(reader.connection().query_row("SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'op'), 0)", [], |row| row.get::<_, i64>(0))?)).unwrap();
    let begun_before = operations_begun(&rig);
    let (entered, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release) = std::sync::mpsc::channel::<()>();
    let expired = Arc::new(AtomicBool::new(false));
    let release_expired = expired.clone();
    *store.after_epoch_check.lock().unwrap() = Some(Box::new(move || { entered.send(()).unwrap(); if release.recv_timeout(std::time::Duration::from_secs(20)).is_err() { release_expired.store(true, Ordering::SeqCst); } }));
    let (committed, committed_rx) = std::sync::mpsc::channel();
    let (publish_tx, publish_release) = std::sync::mpsc::channel::<()>();
    let (committed, publish_release, held) = (Mutex::new(committed), Mutex::new(publish_release), AtomicBool::new(true));
    let publish_expired = expired.clone();
    *store.after_commit.lock().unwrap() = Some(Box::new(move |result| {
        if result.queue_operations.is_empty() || !held.swap(false, Ordering::SeqCst) { return; }
        committed.lock().unwrap().send(()).unwrap();
        if publish_release.lock().unwrap().recv_timeout(std::time::Duration::from_secs(20)).is_err() { publish_expired.store(true, Ordering::SeqCst); }
    }));
    let sender = { let database = rig.database.clone(); tokio::spawn(async move { database.send(message(epoch, "late", 9)).await }) };
    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap()).await.unwrap();
    let waits = *store.lifecycle.begin_waits.0.lock().unwrap();
    assert_eq!(rig.database.session_epoch(), epoch);
    let identity = { let database = rig.database.clone(); tokio::task::spawn_blocking(move || database.db.shared.store.lifecycle.session_from(Some("another-account".into()), database.db.shared.store.lifecycle.admit_passive(crate::lifecycle::Origin::Explicit, "test session", Some(epoch)).unwrap()).is_ok()) };
    yield_until("the session start reached the commit line", || *store.lifecycle.begin_waits.0.lock().unwrap() > waits).await;
    assert_eq!(rig.database.session_epoch(), epoch, "the epoch changed while the commit of the send was held");
    release_tx.send(()).unwrap();
    tokio::task::spawn_blocking(move || committed_rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap()).await.unwrap();
    assert!(bound(identity).await.unwrap(), "the session start of the other identity did not begin");
    publish_tx.send(()).unwrap();
    let sent = bound(sender).await.unwrap();
    assert!(sent.is_ok() || matches!(sent, Err(DbError::Interrupted { .. })), "the send ended with {:?}", sent.as_ref().err());
    assert!(!expired.load(Ordering::SeqCst), "a held step ran out of time and was not released by the test");
    assert_ne!(rig.database.session_epoch(), epoch);
    assert!(operations_begun(&rig) > begun_before, "the send did not commit before the epoch changed");
    settle(&rig).await;
    rig.database.set_online(true).unwrap();
    bound(rig.database.flush()).await.unwrap();
    settle(&rig).await;
    assert_eq!(rig.world.count("SendMessage"), 0, "the message of the account that left went out for the next one");
    assert!(rig.database.db.outbox().unwrap().is_empty(), "an operation of the account that left is still queued");
    rig.database.close_now();
}

/// K02 (B2, old lines ffi/database.rs update, destroy, read_query, read_row): a command or a one-off read that carries the epoch of A is refused after the session ended, also when it waited for the writer while the session ended, and it changes nothing for B.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::await_holding_lock)]
async fn a_command_and_a_read_of_a_left_account_are_refused_and_change_nothing() {
    use crate::lifecycle::{Boundary, End};
    let rig = Rig::encrypted().await;
    rig.world.seed_message("B", "b1", "hello", 1);
    rig.login_ios("A").await;
    let old = rig.database.session_epoch();
    bound(rig.database.session_reset()).await.unwrap();
    rig.login_ios("B").await;
    let current = rig.database.session_epoch();
    assert_ne!(old, current);
    let users = r#"{"model":"User","relation":"all","params":{}}"#;
    let refused = |result: Result<(), DbError>| matches!(result, Err(DbError::Interrupted { .. }));
    assert!(refused(bound(rig.database.read_query(old, users.into())).await.map(|_| ())), "a read of A returned rows in the epoch of B");
    assert!(refused(bound(rig.database.read_row(old, "User".into(), "user".into())).await.map(|_| ())));
    assert!(refused(bound(rig.database.update(old, "User".into(), "user".into(), json!({"name":"hijacked"}).to_string(), "{}".into())).await.map(|_| ())));
    assert!(refused(bound(rig.database.destroy(old, "User".into(), "user".into())).await));
    assert!(refused(bound(rig.database.set_local(old, "User".into(), "user".into(), "name".into(), "\"x\"".into())).await));
    assert!(refused(bound(rig.database.clone().upload_blob(old, "n".into(), "create".into(), crate::ffi::upload_wire::WireUploadRequest { path: "/nowhere".into(), name: None, mime: None, variables_json: "{}".into() })).await.map(|_| ())));
    let seen = bound(rig.database.read_row(current, "User".into(), "user".into())).await.unwrap().expect("the row of B");
    assert!(named(&seen, "B"), "the row of B changed");

    let store = &rig.database.db.shared.store;
    let pinned = store.lifecycle.writes_entered.load(Ordering::SeqCst);
    let writer = store.lock();
    let late = { let database = rig.database.clone(); tokio::spawn(async move { database.update(current, "User".into(), "user".into(), json!({"name":"late"}).to_string(), "{}".into()).await }) };
    yield_until("the update reached the writer", || store.lifecycle.writes_entered.load(Ordering::SeqCst) > pinned).await;
    let permit = store.lifecycle.try_begin(Boundary::SessionEnd(End::Local), Some(current)).ok().and_then(Result::ok).expect("the epoch of the caller is current");
    drop(writer);
    assert!(refused(bound(late).await.unwrap().map(|_| ())), "an update that waited for the writer while the session ended ran");
    drop(permit);
    rig.database.close_now();
}

/// K03 (B3, old lines ffi/command.rs in_epoch with Origin::Explicit and sync/send/flush.rs waiting_reason): a send needs a session, and an action that is not a durable command does not wait for the network: it is refused and rolled back, and it never goes out later.
#[tokio::test(flavor = "multi_thread")]
async fn a_send_needs_a_session_and_a_plain_action_never_waits_for_the_network() {
    let rig = Rig::encrypted().await;
    let epoch = rig.database.session_epoch();
    let sent = bound(rig.database.send(message(epoch, "anonymous", 1))).await;
    assert!(matches!(sent, Err(DbError::NoSession { .. })), "a send without a session was accepted: {sent:?}");
    assert!(rig.database.db.outbox().unwrap().is_empty(), "a send without a session left an operation");
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("g");
    let admitted = rig.database.db.admit().unwrap();
    rig.database.db.create_with(&admitted, "Group", serde_json::from_str(&values).unwrap(), serde_json::from_str(&args).unwrap()).unwrap();
    drop(admitted);
    assert_eq!(rig.database.db.outbox().unwrap().len(), 1, "the plain action was queued once");
    rig.database.set_online(false).unwrap();
    bound(rig.database.flush()).await.unwrap();
    assert!(rig.database.db.outbox().unwrap().is_empty(), "a plain action stayed queued while the network was away");
    assert_eq!(rig.database.db.shared.store.row_count("Group").unwrap(), 0, "the row of the refused action stayed");
    rig.database.set_online(true).unwrap();
    bound(rig.database.flush()).await.unwrap();
    settle(&rig).await;
    assert_eq!(rig.world.count("CreateGroup"), 0, "a plain action went out after the network returned");
    let receipt = bound(rig.database.send(message(epoch, "offline", 2))).await.unwrap();
    assert_eq!(rig.database.command_status(receipt).unwrap(), CommandStatus::Queued);
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn server_calls_without_a_transport_report_no_session() {
    let rig = Rig::encrypted().await;
    bound(rig.database.close_database()).await.unwrap();
    let database = rig.open_again(None).await.unwrap();
    let epoch = database.session_epoch();
    let (values, args) = group("friends");
    let create = bound(database.create(epoch, "Group".into(), values, args)).await;
    let load = bound(database.load(epoch, "User".into(), "profile".into(), "{}".into())).await;
    let send = bound(database.send(message(epoch, "hello", 1))).await;
    let act = bound(database.act(epoch, "Group".into(), "g1".into(), "leave".into(), "{}".into(), "{}".into())).await;
    assert!(matches!(create, Err(DbError::NoSession { .. })), "create: {create:?}");
    assert!(matches!(load, Err(DbError::NoSession { .. })), "load: {load:?}");
    assert!(matches!(send, Err(DbError::NoSession { .. })), "send: {send:?}");
    assert!(matches!(act, Err(DbError::NoSession { .. })));
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn reset_clears_ephemeral_values_before_the_next_account_reads_the_same_id() {
    use crate::schema::{EphemeralSpec, FieldSpec, ModelSpec};
    use crate::value::ScalarKind;
    let mut user: Vec<ModelSpec> = serde_json::from_str(&crate::ffi::lifecycle_tests::models()).unwrap();
    user[0].ephemeral.push(EphemeralSpec::new(FieldSpec::scalar("typing", ScalarKind::Bool).with_default(json!(false))));
    let world = super::rig_tests::World::new("server", "A", true);
    let transport = super::rig_tests::Switchable::new(Some(world.clone()), false);
    let database = Database::open(crate::ffi::Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&user).unwrap(), Some(transport), None, None, None, None, None).await.unwrap();
    database.register_operations(r#"[{"name":"MyProfile","document":"query MyProfile { me { id name } }"}]"#.into()).unwrap();
    let epoch = database.session_epoch();
    bound(database.load(epoch, "User".into(), "profile".into(), "{}".into())).await.unwrap();
    bound(database.set_ephemeral(epoch, "User".into(), "user".into(), r#"{"typing":true}"#.into())).await.unwrap();
    let old = bound(database.read_row(epoch, "User".into(), "user".into())).await.unwrap().unwrap();
    assert!(matches!(old.cells.last(), Some(crate::ffi::Cell::Bool { value: true })));

    bound(database.session_reset()).await.unwrap();
    world.set_account("B");
    bound(database.load(database.session_epoch(), "User".into(), "profile".into(), "{}".into())).await.unwrap();
    let current = bound(database.read_row(database.session_epoch(), "User".into(), "user".into())).await.unwrap().unwrap();

    assert!(named(&current, "B"));
    assert!(matches!(current.cells.last(), Some(crate::ffi::Cell::Bool { value: false })));
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_old_subscription_sink_is_closed_before_the_next_account() {
    let world = super::rig_tests::World::new("server", "A", true);
    let rig = Rig::plain(world.clone(), false).await;
    let epoch = rig.database.session_epoch();
    let stream = rig.database.subscribe(epoch, StreamRequest::Query { query_json: r#"{"model":"Notice","relation":"recent","params":{}}"#.into() }, Reader::open());
    let (token, old_sink) = rig.switch.first_sink().await;

    bound(rig.database.session_reset()).await.unwrap();
    rig.switch.wait_unsubscribed(&token).await;
    world.set_account("B");
    bound(rig.database.load(rig.database.session_epoch(), "User".into(), "profile".into(), "{}".into())).await.unwrap();
    old_sink.event(r#"{"data":{"notices":{"id":"old"}}}"#.into()).unwrap();
    let rows = bound(rig.database.read_query(rig.database.session_epoch(), r#"{"model":"Notice","relation":"recent","params":{}}"#.into())).await.unwrap();

    assert!(rows.is_empty());
    assert!(rig.switch.unsubscribed().contains(&token));
    rig.database.cancel_stream(stream);
    rig.database.close_now();
}

/// K04 (B4, old line ffi/database.rs create): a create the server answers goes through `create` with the epoch of the caller, needs the network, and returns the row the server confirmed.
#[tokio::test(flavor = "multi_thread")]
async fn a_server_create_returns_the_confirmed_row_and_needs_the_network() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let row = bound(rig.database.create(epoch, "Group".into(), values.clone(), args.clone())).await.unwrap();
    assert!(!row.id.starts_with("tmp") && !row.id.contains("temp"), "the created row still has a temporary id: {}", row.id);
    assert!(named(&row, "friends"));
    assert_eq!(rig.world.count("CreateGroup"), 1);
    rig.database.set_online(false).unwrap();
    let offline = bound(rig.database.create(epoch, "Group".into(), values, args)).await;
    assert!(matches!(offline, Err(DbError::Network { .. })), "an offline server create did not fail at once: {offline:?}");
    assert_eq!(rig.world.count("CreateGroup"), 1);
    assert!(rig.database.db.outbox().unwrap().is_empty());
    rig.database.close_now();
}

/// A reader whose last reference decides where the platform object dies.
struct DropProbe { held: Arc<Mutex<Vec<String>>>, dropped: Arc<std::sync::atomic::AtomicUsize> }

impl ValueSink for DropProbe {
    fn delivered(&self, _: StreamDelivery) {}
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.held.lock().unwrap().extend(crate::db::lock::held_classes().into_iter().map(str::to_string));
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

/// K05 (B5, old line stream/owner.rs cancelled): the reader object is released after every library lock, also when the cancel drops the last reference.
#[tokio::test(flavor = "multi_thread")]
async fn the_cancel_of_a_reader_releases_its_object_outside_every_library_lock() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let held = Arc::new(Mutex::new(Vec::new()));
    let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let stream = rig.database.subscribe(epoch, users(), Arc::new(DropProbe { held: held.clone(), dropped: dropped.clone() }));
    bound(rig.database.stream_loaded(stream)).await.unwrap();
    rig.database.cancel_stream(stream);
    assert_eq!(rig.database.stream_census().open, 0);
    yield_until("the reader object was released", || dropped.load(Ordering::SeqCst) == 1).await;
    assert_eq!(dropped.load(Ordering::SeqCst), 1, "the reader object was released more than once");
    assert!(held.lock().unwrap().is_empty(), "the reader object was released under {:?}", held.lock().unwrap());
    rig.database.close_now();
}

/// K06 (B6): the row of a durable command shows in the stream of its reader as pending before the server answers, and the server has not been asked.
#[tokio::test(flavor = "multi_thread")]
async fn a_queued_message_shows_as_pending_in_the_thread_before_the_server_answers() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    let stream = rig.database.subscribe(epoch, thread(), reader.clone());
    bound(rig.database.stream_loaded(stream)).await.unwrap();
    rig.database.set_online(false).unwrap();
    bound(rig.database.send(message(epoch, "queued", 2))).await.unwrap();
    yield_until("the thread shows the queued message", || reader.rows().iter().any(|row| row.sync.state == WireSyncState::Pending)).await;
    assert_eq!(rig.world.count("SendMessage"), 0, "the message was shown only after the server answered");
    assert_eq!(reader.texts().len(), 1);
    rig.database.close_now();
}

/// K07 (B9, old lines ffi/stream/open.rs retain_query and db/observe.rs query_count): a stream that ended leaves no observer and no work behind: a write after it reads nothing and no retained observer is counted.
#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_list_stream_leaves_no_observer_and_a_later_write_does_no_reading() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    let stream = rig.database.subscribe(epoch, thread(), reader.clone());
    bound(rig.database.stream_loaded(stream)).await.unwrap();
    rig.database.cancel_stream(stream);
    let census = rig.database.subscription_census();
    assert_eq!((census.subscriptions, census.observed_queries, census.retained_observers), (0, 0, 0), "a left list stream kept an observer");
    let reads = rig.database.db.counters().observation_reads;
    rig.database.set_online(false).unwrap();
    bound(rig.database.send(message(epoch, "after", 3))).await.unwrap();
    settle(&rig).await;
    assert_eq!(rig.database.db.counters().observation_reads, reads, "a write after the last reader left woke an observer");
    rig.database.close_now();
}


/// K09 (spec B, C; old line ffi/stream/open.rs connect_json with `force` used once at open): a list stream opened with refresh on connect loads again once when the network returns, without a call of the app. A stream without the flag asks nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_that_refreshes_on_connect_loads_again_once_when_the_network_returns() {
    let rig = Rig::encrypted().await;
    rig.world.seed_message("A", "m1", "first", 1);
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let list = Reader::open();
    let stream = rig.database.subscribe(epoch, StreamRequest::Query { query_json: r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"},"refreshOnConnect":true}"#.into() }, list.clone());
    bound(rig.database.stream_loaded(stream)).await.unwrap();
    settle(&rig).await;
    let before = rig.world.count("Thread");
    rig.database.set_online(false).unwrap();
    rig.database.set_online(true).unwrap();
    yield_until("the list loaded again", || rig.world.count("Thread") > before).await;
    settle(&rig).await;
    assert_eq!(rig.world.count("Thread"), before + 1, "the stream loaded more or less than once on reconnect");
    rig.database.cancel_stream(stream);
    rig.database.set_online(false).unwrap();
    rig.database.set_online(true).unwrap();
    for _ in 0..50 { tokio::task::yield_now().await; }
    settle(&rig).await;
    assert_eq!(rig.world.count("Thread"), before + 1, "a cancelled stream loaded on reconnect");
    rig.database.close_now();
}

/// K10 (offline finding A; old line sync/send/pause.rs retry_due): a durable command whose request failed on the network goes out at once when the network returns, without a step of the clock.
#[tokio::test(flavor = "multi_thread")]
async fn a_durable_command_that_failed_on_the_network_goes_out_at_once_when_the_network_returns() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    rig.world.fail_next("SendMessage", super::rig_tests::Fault::Down);
    let receipt = bound(rig.database.send(message(epoch, "queued", 2))).await.unwrap();
    yield_until("the first attempt failed on the network", || rig.database.db.outbox().unwrap().first().is_some_and(|entry| entry.operation.attempts >= 1 && !entry.operation.transport_started)).await;
    assert_eq!(rig.world.count("SendMessage"), 0, "the failed attempt reached the server");
    rig.database.set_online(false).unwrap();
    rig.database.set_online(true).unwrap();
    let outcome = bound(rig.database.command_outcome(receipt)).await.unwrap();
    assert!(matches!(outcome, CommandStatus::Delivered { .. }), "the command did not go out after the network returned: {outcome:?}");
    assert_eq!(rig.world.count("SendMessage"), 1);
    rig.database.close_now();
}

/// K11 (offline finding B; old lines ffi/database.rs refresh and load_more, ffi/stream/mod.rs refresh_stream): a call that always goes to the server fails at once with `Network` while the network is away, and the entity stream shows the error in its state.
#[tokio::test(flavor = "multi_thread")]
async fn a_refresh_and_a_load_more_fail_at_once_offline_and_the_entity_stream_shows_the_error() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let entity = Reader::open();
    let stream = rig.database.subscribe(epoch, StreamRequest::Entity { model: "User".into(), id: "user".into(), fetch: crate::ffi::EntityFetch::Force }, entity.clone());
    bound(rig.database.stream_loaded(stream)).await.unwrap();
    let hits = rig.world.log().len();
    rig.database.set_online(false).unwrap();
    let refreshed = bound(rig.database.refresh_stream(stream)).await;
    assert!(matches!(refreshed, Err(DbError::Network { .. })), "refresh of a stream offline: {refreshed:?}");
    let refreshed = bound(rig.database.refresh(rig.database.session_epoch(), "User".into(), "all".into(), "{}".into())).await;
    assert!(matches!(refreshed, Err(DbError::Network { .. })), "refresh offline: {refreshed:?}");
    let more = bound(rig.database.load_more(rig.database.session_epoch(), "User".into(), "all".into(), "{}".into())).await;
    assert!(matches!(more, Err(DbError::Network { .. })), "load more offline: {more:?}");
    yield_until("the entity stream shows the error", || entity.deliveries().iter().any(|delivery| match &delivery.value {
        StreamValue::State { state } | StreamValue::Snapshot { state, .. } => state.error.is_some(),
        _ => false,
    })).await;
    assert_eq!(rig.world.log().len(), hits, "a call offline reached the server");
    rig.database.close_now();
}

/// K12 (consumer finding forward; old line ffi/database.rs act returned the receiver row): a forward into the same chat and into another chat leaves the source row unchanged, upserts the answer row with its own id in the target chat, and the call returns the answer row. The answer has the shape of the mutation of the app: `messageForward.message` with the id `forwarded-1`.
#[tokio::test(flavor = "multi_thread")]
async fn a_forward_leaves_the_source_unchanged_upserts_the_answer_row_and_returns_it() {
    for (case, target) in [("same chat", "c1"), ("other chat", "c2")] {
        let rig = Rig::encrypted().await;
        rig.world.seed_message("A", "m1", "hello", 1);
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let reader = Reader::open();
        let stream = rig.database.subscribe(epoch, thread(), reader.clone());
        bound(rig.database.stream_loaded(stream)).await.unwrap();
        yield_until("the thread shows the source", || reader.texts().iter().any(|text| text.contains("hello"))).await;
        let before = bound(rig.database.read_row(epoch, "Message".into(), "m1".into())).await.unwrap().expect("the source row");
        let acted = bound(rig.database.act(epoch, "Message".into(), "m1".into(), "forward".into(), "{}".into(), json!({"targetChatId": target}).to_string())).await.unwrap();
        assert_eq!(acted.row.id, "forwarded-1", "{case}: the call returned the source row, not the answer row");
        let after = bound(rig.database.read_row(epoch, "Message".into(), "m1".into())).await.unwrap().expect("the source row");
        assert_eq!(after.cells, before.cells, "{case}: the answer was written onto the source row");
        let forwarded = bound(rig.database.read_row(epoch, "Message".into(), "forwarded-1".into())).await.unwrap().expect("the answer row");
        assert!(forwarded.cells.contains(&crate::ffi::Cell::Text { value: target.into() }), "{case}: the answer row is not in the target chat");
        if target == "c1" {
            yield_until("the thread shows the source and the answer", || reader.rows().len() == 2).await;
        }
        assert!(rig.database.sync.as_ref().unwrap().inner.answers.lock().unwrap().held() == 0, "{case}: a read answer stayed in the map");
        rig.database.close_now();
    }
}

fn groups() -> StreamRequest { StreamRequest::Query { query_json: r#"{"model":"Group","relation":"all","params":{}}"#.into() } }

pub(super) fn member_list(rig: &Rig, group: &str, order: &str) -> Vec<String> {
    let spec = rig.database.db.spec("Member").unwrap();
    let params = json!({"groupId": group, "order": order}).as_object().unwrap().clone();
    let key = spec.selection_key("group_members", &params).unwrap();
    rig.database.db.shared.store.scope_members("Member", key.as_str()).unwrap().into_iter().map(|(id, _)| id).collect()
}

/// The members u1 u2 u3 of the group in both lists (JOINED: u1 u2 u3, OWNER_FIRST: u2 u1 u3) and the row u4 that is in neither list.
pub(super) fn seed_members(rig: &Rig, group: &str) {
    use crate::commit::{CommitEnvelope, EntityOp, ScopeOp, ScopeRef, WriteOrigin};
    use crate::identity::{ModelKey, RowId};
    let db = &rig.database.db;
    let spec = db.spec("Member").unwrap();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for id in ["u1", "u2", "u3", "u4"] {
        let row = json!({"id": id, "name": id}).as_object().unwrap().clone();
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("Member"), id: RowId::new(id).unwrap(), row });
    }
    for (order, ids) in [("JOINED", ["u1", "u2", "u3"]), ("OWNER_FIRST", ["u2", "u1", "u3"])] {
        let params = json!({"groupId": group, "order": order}).as_object().unwrap().clone();
        let scope = ScopeRef::new(ModelKey::new("Member"), spec.selection_key("group_members", &params).unwrap());
        let mut last: Option<String> = None;
        for id in ids {
            let order_key = crate::value::fractional_between(last.as_deref(), None).unwrap();
            last = Some(order_key.clone());
            envelope.scope_ops.push(ScopeOp::Append { scope: scope.clone(), row: RowId::new(id).unwrap(), order_key });
        }
    }
    db.land(&db.admit().unwrap(), envelope).unwrap();
}

fn ids_of(rows: Vec<WireRow>) -> Vec<String> { rows.into_iter().map(|row| row.id).collect() }

pub(super) fn strings(ids: &[&str]) -> Vec<String> { ids.iter().map(|id| id.to_string()).collect() }

/// K32: a member added or removed ends with the member lists of the server in both orders: the added user appears in the lists the screen reads and the removed one leaves them, without a read by the app.
#[tokio::test(flavor = "multi_thread")]
async fn adding_and_removing_a_member_ends_with_the_member_lists_of_the_server_in_both_orders() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let created = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
    let orders = [("JOINED", ["u1", "u2", "u3"]), ("OWNER_FIRST", ["u2", "u1", "u3"])];
    for (order, ids) in orders { rig.world.set_members(order, &ids); }
    let mut readers = Vec::new();
    for (order, _) in orders {
        let (reader, query) = (Reader::open(), json!({"model": "Member", "relation": "group_members", "params": {"groupId": created.id, "order": order}}).to_string());
        let stream = rig.database.subscribe(epoch, StreamRequest::Query { query_json: query }, reader.clone());
        bound(rig.database.stream_loaded(stream)).await.unwrap();
        readers.push(reader);
    }
    let shown = |readers: &[Arc<Reader>]| readers.iter().map(|reader| reader.rows().into_iter().map(|row| row.id).collect::<Vec<_>>()).collect::<Vec<_>>();
    assert_eq!(shown(&readers), [strings(&["u1", "u2", "u3"]), strings(&["u2", "u1", "u3"])], "the lists before the add");
    type Case = (&'static str, [&'static [&'static str]; 2]);
    let cases: [Case; 2] = [("add_member", [&["u1", "u2", "u3", "u9"], &["u2", "u1", "u3", "u9"]]), ("remove_member", [&["u1", "u2", "u3"], &["u2", "u1", "u3"]])];
    for (action, ended) in cases {
        for (index, (order, _)) in orders.iter().enumerate() { rig.world.set_members(order, ended[index]); }
        let acted = bound(rig.database.act(epoch, "Group".into(), created.id.clone(), action.into(), "{}".into(), json!({"userIds": ["u9"]}).to_string())).await;
        assert!(acted.is_ok(), "{action}: the call failed: {:?}", acted.as_ref().err());
        yield_until(&format!("{action}: both lists end with the lists of the server"), || shown(&readers) == [strings(ended[0]), strings(ended[1])]).await;
    }
    rig.database.close_now();
}

/// K13 (B6, C05; old lines: no rule for these actions): add member and remove member change both member lists (JOINED and OWNER_FIRST) at once, before the server answers. The answer keeps the change, a refusal restores the ids and the order of both lists.
#[tokio::test(flavor = "multi_thread")]
async fn member_actions_change_both_member_lists_before_the_answer_and_a_refusal_restores_them() {
    let joined = |ids: &[&str]| ids.iter().map(|id| id.to_string()).collect::<Vec<_>>();
    type Case = (&'static str, &'static str, &'static str, [&'static [&'static str]; 2], [&'static [&'static str]; 2]);
    let cases: [Case; 2] = [
        ("add_member", "AddMember", "u4", [&["u1", "u2", "u3", "u4"], &["u2", "u1", "u3", "u4"]], [&["u1", "u2", "u3"], &["u2", "u1", "u3"]]),
        ("remove_member", "RemoveMember", "u2", [&["u1", "u3"], &["u1", "u3"]], [&["u1", "u2", "u3"], &["u2", "u1", "u3"]]),
    ];
    for (action, operation, user, changed, original) in cases {
        for refusal in [false, true] {
            let case = format!("{action} {}", if refusal { "refused" } else { "answered" });
            let rig = Rig::encrypted().await;
            rig.login_ios("A").await;
            let epoch = rig.database.session_epoch();
            let (values, args) = group("friends");
            let created = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
            seed_members(&rig, &created.id);
            let gate = if refusal { rig.world.hold_then(operation, Fault::Refuse { code: "FORBIDDEN", confirmed: true }) } else { rig.world.hold_next(operation) };
            let acting = { let database = rig.database.clone(); let id = created.id.clone(); tokio::spawn(async move { database.act(epoch, "Group".into(), id, action.into(), "{}".into(), json!({"userIds": [user]}).to_string()).await }) };
            gate.entered_for(&case).await;
            assert!(!acting.is_finished(), "{case}: the call ended before the request reached the server");
            assert_eq!((member_list(&rig, &created.id, "JOINED"), member_list(&rig, &created.id, "OWNER_FIRST")), (joined(changed[0]), joined(changed[1])), "{case}: the lists before the answer");
            gate.release();
            let result = bound(acting).await.unwrap();
            settle(&rig).await;
            let expected = if refusal { original } else { changed };
            assert_eq!((member_list(&rig, &created.id, "JOINED"), member_list(&rig, &created.id, "OWNER_FIRST")), (joined(expected[0]), joined(expected[1])), "{case}: the lists after the outcome");
            assert_eq!(result.is_err(), refusal, "{case}: the outcome of the call was {:?}", result.as_ref().err());
            rig.database.close_now();
        }
    }
}

/// K13 (B6, C05): leave removes the group from the list before the answer. The answer keeps it away, a refusal puts it back.
#[tokio::test(flavor = "multi_thread")]
async fn leave_removes_the_group_from_its_list_before_the_answer_and_a_refusal_restores_it() {
    for refusal in [false, true] {
        let case = if refusal { "refused" } else { "answered" };
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let (values, args) = group("friends");
        let created = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
        let reader = Reader::open();
        let stream = rig.database.subscribe(epoch, groups(), reader.clone());
        bound(rig.database.stream_loaded(stream)).await.unwrap();
        yield_until("the list shows the group", || ids_of(reader.rows()) == strings(&[created.id.as_str()])).await;
        let gate = if refusal { rig.world.hold_then("LeaveGroup", Fault::Refuse { code: "FORBIDDEN", confirmed: true }) } else { rig.world.hold_next("LeaveGroup") };
        let acting = { let database = rig.database.clone(); let id = created.id.clone(); tokio::spawn(async move { database.act(epoch, "Group".into(), id, "leave".into(), "{}".into(), "{}".into()).await }) };
        gate.entered_for(case).await;
        yield_until("the group left the list before the answer", || ids_of(reader.rows()).is_empty()).await;
        gate.release();
        let result = bound(acting).await.unwrap();
        settle(&rig).await;
        assert_eq!(result.is_err(), refusal, "{case}: the outcome of the call was {:?}", result.as_ref().err());
        if refusal {
            yield_until("the refusal put the group back", || ids_of(reader.rows()) == strings(&[created.id.as_str()])).await;
        } else {
            assert!(ids_of(reader.rows()).is_empty(), "{case}: the group came back after the answer");
        }
        rig.database.close_now();
    }
}

/// K13 (B6, C05): forward puts a local copy of the message into the thread of the target chat before the answer. The answer replaces the copy with its own row, a refusal removes the copy.
#[tokio::test(flavor = "multi_thread")]
async fn forward_shows_a_local_copy_in_the_target_thread_and_a_refusal_removes_it() {
    for refusal in [false, true] {
        let case = if refusal { "refused" } else { "answered" };
        let rig = Rig::encrypted().await;
        rig.world.seed_message("A", "m1", "hello", 1);
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let reader = Reader::open();
        let stream = rig.database.subscribe(epoch, thread(), reader.clone());
        bound(rig.database.stream_loaded(stream)).await.unwrap();
        yield_until("the thread shows the source", || ids_of(reader.rows()) == strings(&["m1"])).await;
        let gate = if refusal { rig.world.hold_then("MessageForward", Fault::Refuse { code: "FORBIDDEN", confirmed: true }) } else { rig.world.hold_next("MessageForward") };
        let acting = { let database = rig.database.clone(); tokio::spawn(async move { database.act(epoch, "Message".into(), "m1".into(), "forward".into(), "{}".into(), json!({"targetChatId": "c1"}).to_string()).await }) };
        gate.entered_for(case).await;
        yield_until("the thread shows the copy before the answer", || ids_of(reader.rows()).len() == 2).await;
        let during = ids_of(reader.rows());
        assert!(during.contains(&"m1".to_string()), "{case}: the source left the thread: {during:?}");
        let copy = during.iter().find(|id| id.as_str() != "m1").unwrap();
        assert!(crate::identity::RowId::new(copy).unwrap().is_temp(), "{case}: the copy {copy} is not a temporary row");
        gate.release();
        let result = bound(acting).await.unwrap();
        settle(&rig).await;
        assert_eq!(result.is_err(), refusal, "{case}: the outcome of the call was {:?}", result.as_ref().err());
        let expected = if refusal { strings(&["m1"]) } else { strings(&["m1", "forwarded-1"]) };
        yield_until("the thread shows the outcome", || ids_of(reader.rows()) == expected).await;
        rig.database.close_now();
    }
}

type Call = for<'a> fn(&'a Database, u64) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), DbError>> + 'a>>;

fn case(name: &'static str, call: Call) -> (&'static str, Call) { (name, call) }

/// K17 (C01, old lines ffi/database.rs run, load, refresh, load_all, load_more, node, nodes, retry_all, warm, edge_values, peek_row, cancel_upload and stream/mod.rs peek_query): every entry of the database names the epoch of its caller and is refused after that session ended. Nothing reaches the server.
#[tokio::test(flavor = "multi_thread")]
async fn every_entry_of_a_left_account_is_refused_and_reaches_nothing() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let old = rig.database.session_epoch();
    bound(rig.database.session_reset()).await.unwrap();
    rig.login_ios("B").await;
    let before = rig.world.log().len();
    let calls = vec![
        case("run", |db, old| Box::pin(async move { bound(db.run(old, "MyProfile".into(), "{}".into())).await.map(drop) })),
        case("load", |db, old| Box::pin(async move { bound(db.load(old, "User".into(), "all".into(), "{}".into())).await })),
        case("refresh", |db, old| Box::pin(async move { bound(db.refresh(old, "User".into(), "all".into(), "{}".into())).await })),
        case("refresh_preloads", |db, old| Box::pin(async move { bound(db.refresh_preloads(old)).await })),
        case("load_all", |db, old| Box::pin(async move { bound(db.load_all(old, "User".into(), "all".into(), "{}".into(), true)).await })),
        case("load_more", |db, old| Box::pin(async move { bound(db.load_more(old, "User".into(), "all".into(), "{}".into())).await.map(drop) })),
        case("node", |db, old| Box::pin(async move { bound(db.node(old, "User".into(), "node".into(), "user".into(), true)).await.map(drop) })),
        case("nodes", |db, old| Box::pin(async move { bound(db.nodes(old, "User".into(), "nodes".into(), vec!["user".into()])).await.map(drop) })),
        case("retry_all", |db, old| Box::pin(async move { bound(db.retry_all(old)).await.map(drop) })),
        case("warm", |db, old| Box::pin(async move { bound(db.warm(old, r#"{"model":"User"}"#.into())).await })),
        case("edge_values", |db, old| Box::pin(async move { bound(db.edge_values(old, "User".into(), "all".into(), "{}".into(), vec!["user".into()])).await.map(drop) })),
        case("peek_query", |db, old| Box::pin(async move { bound(db.peek_query(old, r#"{"model":"User"}"#.into())).await.map(drop) })),
        case("peek_row", |db, old| Box::pin(async move { db.peek_row(old, "User".into(), "user".into()).map(drop) })),
        case("cancel_upload", |db, old| Box::pin(async move { bound(db.cancel_upload(old, "User".into(), "user".into(), "upload".into())).await.map(drop) })),
    ];
    for (name, call) in calls {
        let result = call(&rig.database, old).await;
        assert!(matches!(result, Err(DbError::Interrupted { .. })), "{name} of a left account was not refused: {result:?}");
    }
    assert_eq!(rig.world.log().len(), before, "a refused entry reached the server");
    rig.database.close_now();
}

/// K18 (C04, old lines sync/send/flush.rs settle_delivery and online.rs set_online): an action that is not a durable command and whose request never reached the server ends with the network error and rolls back. Nothing replays it when the network returns.
#[tokio::test(flavor = "multi_thread")]
async fn a_plain_action_that_never_reached_the_server_ends_with_the_network_error_and_is_not_replayed() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let created = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
    rig.world.fail_next("LeaveGroup", Fault::Down);
    let acted = bound(rig.database.act(epoch, "Group".into(), created.id.clone(), "leave".into(), "{}".into(), "{}".into())).await;
    assert!(matches!(acted, Err(DbError::Network { .. })), "the action whose request never reached the server did not end with the network error: {:?}", acted.as_ref().err());
    let after = bound(rig.database.read_row(epoch, "Group".into(), created.id.clone())).await.unwrap();
    assert!(after.is_some(), "the group stayed removed after the failed request");
    assert!(rig.database.db.outbox().unwrap().is_empty(), "the failed action stayed queued");
    rig.database.set_online(false).unwrap();
    rig.database.set_online(true).unwrap();
    bound(rig.database.flush()).await.unwrap();
    settle(&rig).await;
    assert_eq!(rig.world.log().iter().filter(|entry| entry.operation == "LeaveGroup").count(), 1, "the action went out again after the network returned");
    rig.database.close_now();
}

/// K19 (C04, old lines sync/send/flush.rs refuse_non_durable and online.rs set_online): an action queued while online and still unsent when the network drops ends with the network error at the drop, also when the network returns before any flush.
#[tokio::test(flavor = "multi_thread")]
async fn a_plain_action_unsent_when_the_network_drops_never_goes_out_after_it_returns() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let created = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
    let sync = rig.database.sync.as_ref().expect("the database has a sync").clone();
    let sender_busy = sync.inner.outbox_gate.lock().await;
    let acting = { let database = rig.database.clone(); let id = created.id.clone(); tokio::spawn(async move { database.act(epoch, "Group".into(), id, "leave".into(), "{}".into(), "{}".into()).await }) };
    yield_until("the action is queued", || !rig.database.db.outbox().unwrap().is_empty()).await;
    rig.database.set_online(false).unwrap();
    rig.database.set_online(true).unwrap();
    drop(sender_busy);
    let acted = bound(acting).await.unwrap();
    assert!(matches!(acted, Err(DbError::Network { .. })), "an action unsent at the drop did not end with the network error: {:?}", acted.as_ref().err());
    bound(rig.database.flush()).await.unwrap();
    settle(&rig).await;
    assert_eq!(rig.world.log().iter().filter(|entry| entry.operation == "LeaveGroup").count(), 0, "an action queued before the drop went out after the network returned");
    assert!(rig.database.db.outbox().unwrap().is_empty());
    rig.database.close_now();
}

/// K20 (C08, old line sync/send/pause.rs retry_due): after a network return the first retry skips the backoff once. A second failure waits by the normal backoff, and the clock then delivers the command.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_failure_after_a_network_return_waits_by_the_backoff_and_the_clock_delivers() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    rig.world.fail_next("SendMessage", super::rig_tests::Fault::Down);
    rig.world.fail_next("SendMessage", super::rig_tests::Fault::Down);
    let receipt = bound(rig.database.send(message(epoch, "queued", 2))).await.unwrap();
    let attempts = |rig: &Rig| rig.world.log().iter().filter(|entry| entry.operation == "SendMessage").count();
    let failed_and_waiting = |rig: &Rig, count: usize| attempts(rig) == count && rig.database.db.outbox().unwrap().first().is_some_and(|entry| entry.operation.attempts as usize >= count && !entry.operation.transport_started);
    yield_until("the first request failed and the command waits", || failed_and_waiting(&rig, 1)).await;
    rig.database.set_online(false).unwrap();
    rig.database.set_online(true).unwrap();
    yield_until("the retry after the network return failed and the command waits", || failed_and_waiting(&rig, 2)).await;
    settle(&rig).await;
    assert_eq!(attempts(&rig), 2, "the command was tried again before its backoff");
    rig.clock.advance(3_600_000);
    let outcome = bound(rig.database.command_outcome(receipt)).await.unwrap();
    assert!(matches!(outcome, CommandStatus::Delivered { .. }), "the clock did not deliver the command: {outcome:?}");
    rig.database.close_now();
}

/// K23 (D01, old lines ffi/command.rs serve_in_epoch and sync/land.rs land_rows): a request that was accepted in the epoch of A, and whose answer waits at the server while another identity starts a session, lands nothing and ends refused. The epoch of A does not change, the generation does not either.
#[tokio::test(flavor = "multi_thread")]
async fn an_accepted_request_whose_answer_lands_after_the_next_account_started_writes_nothing() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    bound(rig.database.refresh(epoch, "User".into(), "profile".into(), "{}".into())).await.unwrap();
    let before = bound(rig.database.read_row(epoch, "User".into(), "user".into())).await.unwrap().expect("the row of A");
    assert!(named(&before, "A"));
    let gate = rig.world.hold_next("MyProfile");
    gate.as_account("late");
    let running = { let database = rig.database.clone(); tokio::spawn(async move { database.refresh(epoch, "User".into(), "profile".into(), "{}".into()).await }) };
    gate.entered_for("the accepted request").await;
    let store = &rig.database.db.shared.store;
    let moved = { let database = rig.database.clone(); tokio::task::spawn_blocking(move || database.db.shared.store.lifecycle.session_from(Some("another-account".into()), database.db.shared.store.lifecycle.admit_passive(crate::lifecycle::Origin::Explicit, "test session", Some(epoch)).unwrap()).is_ok()) };
    assert!(bound(moved).await.unwrap(), "the session start of the other identity did not begin");
    assert_ne!(rig.database.session_epoch(), epoch);
    gate.release();
    let result = bound(running).await.unwrap();
    assert!(matches!(result, Err(DbError::Interrupted { .. })), "the request of the epoch that left ended with {result:?}");
    settle(&rig).await;
    let after = bound(rig.database.read_row(rig.database.session_epoch(), "User".into(), "user".into())).await.unwrap().expect("the row");
    assert!(named(&after, "A") && !named(&after, "late"), "the answer of the epoch that left was written");
    assert_eq!(store.row_count("User").unwrap(), 1);
    rig.database.close_now();
}

/// K24 (D03, old lines sync/send/flush.rs flush and next_ready): a flush that is already running does not send an action that is not a durable command after the network dropped and returned between two of its sends.
#[tokio::test(flavor = "multi_thread")]
async fn a_running_flush_does_not_send_a_plain_action_after_the_network_dropped_between_two_sends() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let created = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
    let gate = rig.world.hold_next("SendMessage");
    bound(rig.database.send(message(epoch, "first", 1))).await.unwrap();
    gate.entered_for("the first send").await;
    let acting = { let database = rig.database.clone(); let id = created.id.clone(); tokio::spawn(async move { database.act(epoch, "Group".into(), id, "leave".into(), "{}".into(), "{}".into()).await }) };
    yield_until("the action is queued behind the send", || rig.database.db.outbox().unwrap().len() == 2).await;
    rig.database.set_online(false).unwrap();
    rig.database.set_online(true).unwrap();
    gate.release();
    let acted = bound(acting).await.unwrap();
    assert!(matches!(acted, Err(DbError::Network { .. })), "the action behind a send did not end with the network error: {:?}", acted.as_ref().err());
    settle(&rig).await;
    assert_eq!(rig.world.log().iter().filter(|entry| entry.operation == "LeaveGroup").count(), 0, "a running flush sent the action after the network dropped");
    rig.database.close_now();
}

/// K25 (D04, old lines db/roll_back.rs restore and db/prior.rs reappearing): two operations change the same member of the same lists and both are refused. The lists end the way they were, in both orders of the two changes, the first refusal arriving while the second still holds its answer.
#[tokio::test(flavor = "multi_thread")]
async fn two_refused_changes_of_one_member_restore_the_original_lists_in_both_orders() {
    type Case = (&'static str, [(&'static str, &'static str); 2], &'static str, [&'static [&'static str]; 2], [&'static [&'static str]; 2]);
    let cases: [Case; 2] = [
        ("remove then add a member", [("remove_member", "RemoveMember"), ("add_member", "AddMember")], "u2", [&["u1", "u3", "u2"], &["u1", "u3", "u2"]], [&["u1", "u2", "u3"], &["u2", "u1", "u3"]]),
        ("add then remove a member", [("add_member", "AddMember"), ("remove_member", "RemoveMember")], "u4", [&["u1", "u2", "u3"], &["u2", "u1", "u3"]], [&["u1", "u2", "u3"], &["u2", "u1", "u3"]]),
    ];
    for (case, [first, second], user, while_second, original) in cases {
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let (values, args) = group("friends");
        let created = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
        seed_members(&rig, &created.id);
        let (first_gate, second_gate) = (rig.world.hold_then(first.1, Fault::Refuse { code: "FORBIDDEN", confirmed: true }), rig.world.hold_then(second.1, Fault::Refuse { code: "FORBIDDEN", confirmed: true }));
        let args = json!({"userIds": [user]}).to_string();
        let start = |action: &'static str| { let database = rig.database.clone(); let (id, args) = (created.id.clone(), args.clone()); tokio::spawn(async move { database.act(epoch, "Group".into(), id, action.into(), "{}".into(), args).await }) };
        let first_call = start(first.0);
        first_gate.entered_for(case).await;
        let second_call = start(second.0);
        yield_until("the second change is queued", || rig.database.db.outbox().unwrap().len() == 2).await;
        first_gate.release();
        second_gate.entered_for(case).await;
        let lists = |rig: &Rig| (member_list(rig, &created.id, "JOINED"), member_list(rig, &created.id, "OWNER_FIRST"));
        let expected = |lists: [&[&str]; 2]| (strings(lists[0]), strings(lists[1]));
        assert_eq!(lists(&rig), expected(while_second), "{case}: the lists while the second change waits for its answer");
        second_gate.release();
        assert!(bound(first_call).await.unwrap().is_err() && bound(second_call).await.unwrap().is_err(), "{case}: a refused change succeeded");
        settle(&rig).await;
        assert_eq!(lists(&rig), expected(original), "{case}: the lists after both refusals");
        rig.database.close_now();
    }
}

/// K26 (E04 and the outcome of an accepted call, old lines db/local_write.rs join_scope_rows and leave_scope_rows, sync/send/mod.rs settled): the same change of a member made twice, once the first and once the second refused, leaves the member the way the accepted change wants, also while the second still holds its answer. The accepted call returns its success and the refused call its refusal.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_change_of_a_member_does_not_undo_the_same_change_accepted_and_each_call_gets_its_own_outcome() {
    type Case = (&'static str, (&'static str, &'static str), &'static str, [&'static [&'static str]; 2], bool);
    let cases: [Case; 4] = [
        ("add twice, first refused", ("add_member", "AddMember"), "u4", [&["u1", "u2", "u3", "u4"], &["u2", "u1", "u3", "u4"]], true),
        ("remove twice, first refused", ("remove_member", "RemoveMember"), "u2", [&["u1", "u3"], &["u1", "u3"]], true),
        ("add twice, second refused", ("add_member", "AddMember"), "u4", [&["u1", "u2", "u3", "u4"], &["u2", "u1", "u3", "u4"]], false),
        ("remove twice, second refused", ("remove_member", "RemoveMember"), "u2", [&["u1", "u3"], &["u1", "u3"]], false),
    ];
    for (case, (action, operation), user, expected, first_refused) in cases {
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let (values, args) = group("friends");
        let created = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
        seed_members(&rig, &created.id);
        let held = |refused: bool| if refused { rig.world.hold_then(operation, Fault::Refuse { code: "FORBIDDEN", confirmed: true }) } else { rig.world.hold_next(operation) };
        let first_gate = held(first_refused);
        let second_gate = held(!first_refused);
        let args = json!({"userIds": [user]}).to_string();
        let start = || { let database = rig.database.clone(); let (id, args) = (created.id.clone(), args.clone()); tokio::spawn(async move { database.act(epoch, "Group".into(), id, action.into(), "{}".into(), args).await }) };
        let first_call = start();
        first_gate.entered_for(case).await;
        let second_call = start();
        yield_until("the second change is queued", || rig.database.db.outbox().unwrap().len() == 2).await;
        first_gate.release();
        second_gate.entered_for(case).await;
        let lists = |rig: &Rig| (member_list(rig, &created.id, "JOINED"), member_list(rig, &created.id, "OWNER_FIRST"));
        let wanted = (strings(expected[0]), strings(expected[1]));
        assert_eq!(lists(&rig), wanted, "{case}: the lists while the second change waits for its answer");
        second_gate.release();
        let (first, second) = (bound(first_call).await.unwrap(), bound(second_call).await.unwrap());
        assert_eq!((first.is_err(), second.is_err()), (first_refused, !first_refused), "{case}: the calls ended with {:?} and {:?}", first.as_ref().err(), second.as_ref().err());
        settle(&rig).await;
        assert_eq!(lists(&rig), wanted, "{case}: the lists after both answers");
        rig.database.close_now();
    }
}

/// K27 (E02, old lines sync/send/flush.rs flush_pass and refuse_non_durable): a network that dropped and returned right after a flush decided which actions to refuse refuses a plain create and a plain action, and nothing of them goes out.
#[tokio::test(flavor = "multi_thread")]
async fn a_network_that_dropped_and_returned_right_after_the_refusal_decision_refuses_every_plain_action() {
    use std::sync::atomic::{AtomicBool, Ordering};
    for case in ["create", "leave", "add_member"] {
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let (values, args) = group("friends");
        let created = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
        seed_members(&rig, &created.id);
        settle(&rig).await;
        let operation = match case { "create" => "CreateGroup", "leave" => "LeaveGroup", _ => "AddMember" };
        let sent_before = rig.world.count(operation);
        let (database, once) = (rig.database.clone(), AtomicBool::new(false));
        *rig.database.sync.as_ref().unwrap().inner.refusal_probe.lock().unwrap() = Some(Box::new(move || {
            if !once.swap(true, Ordering::SeqCst) {
                database.set_online(false).unwrap();
                database.set_online(true).unwrap();
            }
        }));
        let result = match case {
            "create" => { let (values, args) = group("second"); bound(rig.database.create(epoch, "Group".into(), values, args)).await.map(|_| ()) }
            "leave" => bound(rig.database.act(epoch, "Group".into(), created.id.clone(), "leave".into(), "{}".into(), "{}".into())).await.map(|_| ()),
            _ => bound(rig.database.act(epoch, "Group".into(), created.id.clone(), "add_member".into(), "{}".into(), json!({"userIds": ["u4"]}).to_string())).await.map(|_| ()),
        };
        assert!(result.is_err(), "{case}: the action succeeded although the network dropped and returned after the refusal decision");
        settle(&rig).await;
        assert_eq!(rig.world.count(operation), sent_before, "{case}: the action went out after the network dropped and returned");
        rig.database.close_now();
    }
}

/// K29 (F02, old lines sync/send/flush.rs start and store/apply.rs attempt_in_epoch): a network that moves after the plan of the start of an action and before its commit refuses the start, and the action does not go out.
#[tokio::test(flavor = "multi_thread")]
async fn a_network_that_moves_between_the_plan_and_the_commit_of_a_start_refuses_the_action() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let created = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
    settle(&rig).await;
    let sent_before = rig.world.count("LeaveGroup");
    let (shared, inner) = (rig.database.db.shared.clone(), rig.database.sync.as_ref().unwrap().inner.clone());
    let armed = inner.clone();
    *inner.refusal_probe.lock().unwrap() = Some(Box::new(move || {
        let moving = armed.clone();
        *shared.store.after_epoch_check.lock().unwrap() = Some(Box::new(move || { *moving.network_lost.lock().unwrap() = i64::MAX; }));
    }));
    let result = bound(rig.database.act(epoch, "Group".into(), created.id.clone(), "leave".into(), "{}".into(), "{}".into())).await;
    assert!(matches!(result, Err(DbError::Network { .. })), "the action ended with {:?}", result.as_ref().err());
    settle(&rig).await;
    assert_eq!(rig.world.count("LeaveGroup"), sent_before, "the action went out although the network moved before its start was committed");
    rig.database.close_now();
}

/// K30 (F01, old lines ffi/database.rs flush and sync/send/flush.rs flush_pass): the public flush of a queued durable command lands its SQL commits in the epoch of its own lease, so a session start waits for the commit that is running and the epoch does not change under it.
#[tokio::test(flavor = "multi_thread")]
async fn a_public_flush_holds_the_commit_line_of_its_epoch_while_it_commits() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    rig.database.set_online(false).unwrap();
    bound(rig.database.send(message(epoch, "queued", 1))).await.unwrap();
    let store = &rig.database.db.shared.store;
    let (entered, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release) = std::sync::mpsc::channel::<()>();
    *store.after_epoch_check.lock().unwrap() = Some(Box::new(move || { entered.send(()).unwrap(); let _ = release.recv_timeout(std::time::Duration::from_secs(20)); }));
    rig.database.db.connectivity().set_online(true);
    let flushing = { let database = rig.database.clone(); tokio::spawn(async move { database.flush().await }) };
    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap()).await.unwrap();
    let waits = *store.lifecycle.begin_waits.0.lock().unwrap();
    let identity = { let database = rig.database.clone(); tokio::task::spawn_blocking(move || database.db.shared.store.lifecycle.session_from(Some("another-account".into()), database.db.shared.store.lifecycle.admit_passive(crate::lifecycle::Origin::Explicit, "test session", Some(epoch)).unwrap()).is_ok()) };
    yield_until("the session start reached the commit line", || *store.lifecycle.begin_waits.0.lock().unwrap() > waits).await;
    assert_eq!(rig.database.session_epoch(), epoch, "the epoch changed while a commit of the flush was held");
    release_tx.send(()).unwrap();
    let _ = bound(flushing).await.unwrap();
    assert!(bound(identity).await.unwrap(), "the session start of the other identity did not begin");
    settle(&rig).await;
    rig.database.close_now();
}
