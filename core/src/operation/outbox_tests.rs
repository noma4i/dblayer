use super::*;
use crate::identity::{ModelKey, RowId, RowRef};
use OperationStatus::*;

fn id(value: &str) -> RowId {
    RowId::new(value).unwrap()
}

fn operation(seq: i64, model: &str, status: OperationStatus, rows: &[&str], temp: Option<&str>, started: bool) -> Operation {
    let mut operation = Operation::new(&Operation::action_key(model, "send"), rows.iter().map(|r| id(r)).collect(), 0, seq);
    operation.id = OperationId(seq);
    operation.status = status;
    operation.temp_id = temp.map(id);
    operation.transport_started = started;
    operation
}

#[test]
fn verdict_follows_entry_and_queue_state() {
    let pending = OutboxEntry::new(1, Pending, false);
    let unknown = OutboxEntry::new(1, DeliveryUnknown, true);
    assert_eq!(transport_verdict(None, false, false), TransportVerdict::SkipClosed);
    assert_eq!(transport_verdict(Some(&pending), true, false), TransportVerdict::JoinInFlight);
    assert_eq!(transport_verdict(Some(&unknown), false, false), TransportVerdict::SkipUnknownDelivery);
    assert_eq!(transport_verdict(Some(&pending), false, true), TransportVerdict::BlockedByHead);
    assert_eq!(transport_verdict(Some(&pending), false, false), TransportVerdict::Send);
}

#[test]
fn recovery_blocks_followers_and_ready_list_sorts_by_sequence() {
    let entries = [
        OutboxEntry::new(4, Pending, false),
        OutboxEntry::new(1, Pending, false),
        OutboxEntry::new(3, Pending, true),
        OutboxEntry::new(2, Committed, false),
    ];
    assert!(blocks_followers(&OutboxEntry::new(1, Pending, true)));
    assert!(!blocks_followers(&OutboxEntry::new(2, Pending, false)));
    assert!(!blocks_followers(&OutboxEntry::new(3, Committed, true)));
    assert_eq!(ready(&entries), [1, 4]);
}

#[test]
fn only_send_counts_transport_and_terminal_status_releases_input() {
    assert!(counts_transport_call(TransportVerdict::Send));
    for verdict in [TransportVerdict::SkipUnknownDelivery, TransportVerdict::SkipClosed, TransportVerdict::JoinInFlight, TransportVerdict::BlockedByHead] {
        assert!(!counts_transport_call(verdict));
    }
    assert!(releases_input(Committed));
    assert!(releases_input(RolledBack));
    assert!(!releases_input(Failed));
    assert!(!releases_input(Pending));
}

#[test]
fn open_row_ids_include_pending_rows_and_temp_id() {
    let queue = OutboxQueue::new(vec![
        operation(1, "messages", Pending, &["1"], Some("2"), false),
        operation(2, "messages", Committed, &["3"], None, false),
    ]);
    assert_eq!(queue.open_row_ids(&ModelKey::new("messages")), [id("1"), id("2")].into_iter().collect());
}

#[test]
fn open_row_ids_for_other_model_is_empty() {
    let queue = OutboxQueue::new(vec![operation(1, "profiles", Pending, &["1"], None, false)]);
    assert!(queue.open_row_ids(&ModelKey::new("messages")).is_empty());
}

#[test]
fn head_returns_lowest_sequence_for_row() {
    let row = RowRef::new(ModelKey::new("messages"), id("1"));
    let queue = OutboxQueue::new(vec![operation(2, "messages", Pending, &["1"], None, false), operation(1, "messages", Pending, &["1"], None, false)]);
    assert_eq!(queue.head(&row).map(|o| o.id), Some(OperationId(1)));
    let temp = RowRef::new(ModelKey::new("messages"), id("t"));
    let queue = OutboxQueue::new(vec![operation(1, "messages", Pending, &[], Some("t"), false)]);
    assert_eq!(queue.head(&temp).map(|o| o.id), Some(OperationId(1)));
}

#[test]
fn is_blocked_follows_head_status_and_transport() {
    let row = RowRef::new(ModelKey::new("messages"), id("1"));
    assert!(OutboxQueue::new(vec![operation(1, "messages", DeliveryUnknown, &["1"], None, true)]).is_blocked(&row));
    assert!(!OutboxQueue::new(vec![operation(1, "messages", Pending, &["1"], None, false)]).is_blocked(&row));
    assert!(!OutboxQueue::new(vec![]).is_blocked(&row));
}
