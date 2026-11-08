use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;
use serde_json::json;
use super::rig_tests::{bound, Rig};
use crate::ffi::tests::lifecycle_tests::named;
use crate::ffi::tape::StreamDelivery;
use crate::ffi::{CommandRequest, ReadEnd, ReadRequest, ReadValue, RowEdit, WireOperationOutcome, WireRow};

/// What a platform reader holds: the deliveries in order, replayed into the rows its screen shows.
#[derive(Default)]
pub(super) struct Reader {
    pub(super) log: Mutex<Vec<StreamDelivery>>,
    pub(super) threads: Mutex<Vec<ThreadId>>,
    pub(super) lose_next: AtomicBool,
    pub(super) changed: tokio::sync::Notify,
}

impl crate::ffi::tape::Reader for Reader {
    fn delivered(&self, delivery: StreamDelivery) {
        self.threads.lock().unwrap().push(std::thread::current().id());
        if self.lose_next.swap(false, Ordering::SeqCst) { return; }
        self.log.lock().unwrap().push(delivery);
        self.changed.notify_one();
    }
}

impl Reader {
    pub(super) fn open() -> Arc<Reader> { Arc::new(Reader::default()) }
    pub(super) fn deliveries(&self) -> Vec<StreamDelivery> { self.log.lock().unwrap().clone() }
    pub(super) fn len(&self) -> usize { self.log.lock().unwrap().len() }

    /// The rows after the last snapshot and the deltas that chain from it. A delta whose base is not the held revision is a gap and is counted.
    pub(super) fn replay(&self) -> (Vec<WireRow>, usize) {
        let (mut rows, mut revision, mut gaps) = (Vec::new(), None, 0);
        for delivery in self.deliveries() {
            match delivery.value {
                ReadValue::Snapshot { revision: held, rows: whole, .. } => { rows = whole; revision = Some(held); }
                ReadValue::Delta { base, revision: next, edits, .. } => {
                    if revision != Some(base) { gaps += 1; continue; }
                    RowEdit::replay(&mut rows, &edits);
                    revision = Some(next);
                }
                _ => {}
            }
        }
        (rows, gaps)
    }

    pub(super) fn rows(&self) -> Vec<WireRow> { self.replay().0 }
    pub(super) fn gaps(&self) -> usize { self.replay().1 }
    pub(super) fn shows(&self, account: &str) -> bool { let rows = self.rows(); !rows.is_empty() && rows.iter().all(|row| named(row, account)) }
    pub(super) fn ever_showed(&self, account: &str) -> bool {
        self.deliveries().iter().any(|delivery| match &delivery.value {
            ReadValue::Snapshot { rows, .. } => rows.iter().any(|row| named(row, account)),
            ReadValue::Delta { edits, .. } => crate::ffi::RowEdit::carried(edits).iter().any(|row| named(row, account)),
            _ => false,
        })
    }
    pub(super) fn ended(&self) -> Vec<ReadEnd> {
        self.deliveries().into_iter().filter_map(|delivery| if let ReadValue::Ended { cause } = delivery.value { Some(cause) } else { None }).collect()
    }
    pub(super) fn texts(&self) -> Vec<String> {
        self.rows().iter().map(|row| row.cells.iter().filter_map(|cell| if let crate::ffi::Cell::Text { value } = cell { Some(value.clone()) } else { None }).collect::<Vec<_>>().join("|")).collect()
    }
}

pub(super) fn profile() -> ReadRequest { ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"User","relation":"profile","params":{}}"#) } }

pub(super) fn users() -> ReadRequest { ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"User","relation":"all","params":{}}"#) } }

pub(super) fn thread() -> ReadRequest {
    ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"},"order":[{"field":"at","direction":"asc"}]}"#) }
}

pub(super) fn message(epoch: u64, text: &str, at: i64) -> CommandRequest {
    CommandRequest { epoch, model: "Message".into(), values_json: json!({"chatId":"c1","text":text,"at":at}).to_string(), args_json: "{}".into(), upload: None }
}

pub(super) async fn settle(rig: &Rig) { rig.settle().await; }

pub(super) fn group(name: &str) -> (String, String) { (json!({"name": name}).to_string(), "{}".into()) }

pub(super) async fn confirmed_group(rig: &Rig, epoch: u64, values: String, args: String) -> WireRow {
    let receipt = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
    match bound(rig.database.outcome(receipt)).await.unwrap() {
        WireOperationOutcome::Confirmed { value: Some(row), .. } => row,
        other => panic!("group create did not confirm: {other:?}"),
    }
}

pub(super) fn member_list(rig: &Rig, group: &str, order: &str) -> Vec<String> {
    let spec = rig.database.db.spec("Member").unwrap();
    let params = json!({"groupId": group, "order": order}).as_object().unwrap().clone();
    let key = spec.selection_key("group_members", &params).unwrap();
    rig.database.db.scope_members("Member", key.as_str()).unwrap().into_iter().map(|(id, _)| id).collect()
}

/// The server holds the members u1 u2 u3 of the group in both lists (JOINED: u1 u2 u3, OWNER_FIRST: u2 u1 u3) and the row u4 in a third list that no action touches. The core reads the three lists.
pub(super) async fn seed_members(rig: &Rig, group: &str) {
    for (order, ids) in [("JOINED", &["u1", "u2", "u3"][..]), ("OWNER_FIRST", &["u2", "u1", "u3"][..]), ("SPARE", &["u4"][..])] {
        rig.world.set_members(order, ids);
        let params = json!({"groupId": group, "order": order}).to_string();
        bound(rig.database.refresh(rig.database.session_epoch(), "Member".into(), Some("group_members".into()), params)).await.unwrap();
    }
}

pub(super) fn ids_of(rows: Vec<WireRow>) -> Vec<String> { rows.into_iter().map(|row| row.id).collect() }

pub(super) fn strings(ids: &[&str]) -> Vec<String> { ids.iter().map(|id| id.to_string()).collect() }
