use std::collections::BTreeMap;

use crate::owners::contract::{BatchAuthor, EpochEnded};
use crate::owners::store::{BootFound, Done, StoreBatch, StoreEffect, StoreInput, StoreState, StoreWriter};
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
    let effects = step(&mut state, StoreInput::Written { author: BatchAuthor::Outbox, batch: 1, result: Ok(Done { revision: 5 }) });
    assert!(effects.contains(&StoreEffect::Pinned { epoch: 0, request_id: 1, base_revision: 5 }));
    assert_eq!(limit(&step(&mut state, batch(2))), Some(Some(5)));
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
