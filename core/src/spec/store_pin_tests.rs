use std::collections::BTreeMap;

use crate::owners::contract::{BatchAuthor, EpochEnded};
use crate::owners::store::{BootFound, Done, StoreBatch, StoreEffect, StoreInput, StoreState, StoreWriter, WriteDone};
use crate::runtime::{Cause, Effect, StateOwner};

fn step(state: &mut StoreState, input: StoreInput) -> Vec<StoreEffect> {
    StoreWriter::step(state, input).into_iter().filter_map(|effect| match effect { Effect::Run(effect) => Some(effect), _ => None }).collect()
}

fn state(revision: u64) -> StoreState {
    let mut state = StoreState::new();
    step(&mut state, StoreInput::Boot(BootFound::Same { saved_epoch: 0, saved_generation: 0, saved_identity: None, revision }));
    state
}

fn batch(id: u64) -> StoreInput {
    StoreInput::Commit(StoreBatch { author: BatchAuthor::Outbox, id, epoch: 0, basis: BTreeMap::new(), rows: Vec::new(), intents: 0, credential: None, ops: Vec::new() })
}

fn pin(state: &mut StoreState, epoch: u64, request_id: u64) -> Vec<StoreEffect> {
    step(state, StoreInput::Pin { epoch, request_id })
}

fn limit(effects: &[StoreEffect]) -> Option<Option<u64>> {
    effects.iter().find_map(|effect| match effect { StoreEffect::Write { min_pin_revision, .. } => Some(*min_pin_revision), _ => None })
}

#[test]
fn store_pin_holds_tombstones() {
    let mut state = state(4);
    assert_eq!(pin(&mut state, 0, 1), vec![StoreEffect::Pinned { epoch: 0, request_id: 1, base_revision: 4 }]);
    assert_eq!(limit(&step(&mut state, batch(1))), Some(Some(4)));
}

#[test]
fn store_pin_stale_epoch() {
    let mut state = state(4);
    assert_eq!(pin(&mut state, 1, 1), vec![StoreEffect::Pinned { epoch: 1, request_id: 1, base_revision: 4 }]);
    assert_eq!(limit(&step(&mut state, batch(1))), Some(None));
}

#[test]
fn store_pin_deferred_while_writing() {
    let mut state = state(4);
    step(&mut state, batch(1));
    assert!(pin(&mut state, 0, 1).is_empty());
    let effects = step(&mut state, StoreInput::Written { author: BatchAuthor::Outbox, batch: 1, result: Ok(WriteDone { revision: 5, ..WriteDone::default() }) });
    assert!(effects.contains(&StoreEffect::Pinned { epoch: 0, request_id: 1, base_revision: 5 }));
    assert_eq!(limit(&step(&mut state, batch(2))), Some(Some(5)));

    let mut state = self::state(4);
    let effects = step(&mut state, StoreInput::ClearModels { epoch: 0, generation_new: 1, models: ["Item".to_string()].into_iter().collect(), erase_id: 1 });
    let io_id = effects.iter().find_map(|effect| match effect { StoreEffect::EraseModels { io_id, .. } => Some(*io_id), _ => None }).unwrap();
    assert!(pin(&mut state, 0, 2).is_empty());
    let effects = step(&mut state, StoreInput::Erased { io_id, result: Ok(Done { revision: 5 }) });
    assert!(effects.contains(&StoreEffect::Pinned { epoch: 0, request_id: 2, base_revision: 5 }));
}

#[test]
fn store_unpin_releases() {
    let mut state = state(4);
    pin(&mut state, 0, 1);
    assert!(step(&mut state, StoreInput::Unpin { request_id: 1 }).is_empty());
    assert_eq!(limit(&step(&mut state, batch(1))), Some(None));
}

#[test]
fn store_unpin_unknown() {
    let mut state = state(4);
    pin(&mut state, 0, 1);
    assert!(step(&mut state, StoreInput::Unpin { request_id: 2 }).is_empty());
    assert_eq!(limit(&step(&mut state, batch(1))), Some(Some(4)));
}

#[test]
fn store_epoch_ended_drops_pins() {
    let mut state = state(4);
    pin(&mut state, 0, 1);
    step(&mut state, StoreInput::EpochEnded(EpochEnded { epoch: 0, cause: Cause::SessionReset }));
    let mut input = batch(1);
    if let StoreInput::Commit(ref mut batch) = input { batch.epoch = 1; }
    assert_eq!(limit(&step(&mut state, input)), Some(None));
}

#[test]
fn store_prop_tombstone_flight() {
    use std::sync::Arc;
    use std::time::Duration;
    use crate::identity::RowId;
    use crate::owners::read::windows::WindowKey;
    use crate::owners::store::{RowValue, StoreOp};

    let db = crate::db::Db::open_in_memory(crate::db::fixtures::all_specs(), None).unwrap();
    let clock = Arc::new(crate::pacing::ManualClock::new(Duration::from_secs(1)));
    db.set_clock(clock.clone());
    let row = |id: &str| RowValue { row_id: id.into(), values: format!(r#"{{"id":"{id}","text":"value"}}"#) };
    db.write_queue_ops_with_pin(&[StoreOp::Rows { model: "moments".into(), rows: vec![row("m1")] }], None).unwrap();
    let base = u64::try_from(db.revision_seq().unwrap()).unwrap();
    let mut owner = state(base);
    assert_eq!(pin(&mut owner, 0, 7), vec![StoreEffect::Pinned { epoch: 0, request_id: 7, base_revision: base }]);
    let min = limit(&step(&mut owner, batch(1))).unwrap();
    db.land_events_with_pin(&[("moments".into(), r#"{"op":"delete","id":"m1"}"#.into())], min).unwrap();
    assert!(db.is_tombstoned("moments", "m1").unwrap());
    clock.advance(Duration::from_secs(86_401));
    db.write_queue_ops_with_pin(&[StoreOp::Rows { model: "moments".into(), rows: vec![row("m2")] }], min).unwrap();
    assert!(db.is_tombstoned("moments", "m1").unwrap());
    db.write_queue_ops_with_pin(&[StoreOp::AttachFile { model: "moments".into(), row_id: "m2".into(), field: "media".into(), bytes: b"file".to_vec(), name: "file.txt".into(), mime: "text/plain".into() }], min).unwrap();
    assert!(db.is_tombstoned("moments", "m1").unwrap());
    db.attach(&db.admit().unwrap(), "moments", &RowId::new("m2").unwrap(), "cover", b"other", "other.txt", "text/plain").unwrap();
    assert!(db.is_tombstoned("moments", "m1").unwrap());
    let key = WindowKey::nodes("moments", &["m1"]);
    db.write_queue_ops_with_pin(&[StoreOp::Window { key: key.name, model: key.model, page: 1, replace_all: false, cursor: None, roots: vec![row("m1")], related: vec![], total: None, base_revision: i64::try_from(base).unwrap() }], min).unwrap();
    assert!(db.find("moments", &RowId::new("m1").unwrap()).unwrap().is_none());
    step(&mut owner, StoreInput::Written { author: BatchAuthor::Outbox, batch: 1, result: Ok(WriteDone { revision: u64::try_from(db.revision_seq().unwrap()).unwrap(), ..WriteDone::default() }) });
    step(&mut owner, StoreInput::Unpin { request_id: 7 });
    let min = limit(&step(&mut owner, batch(2))).unwrap();
    db.write_queue_ops_with_pin(&[StoreOp::Rows { model: "moments".into(), rows: vec![row("m3")] }], min).unwrap();
    assert!(!db.is_tombstoned("moments", "m1").unwrap());
}
