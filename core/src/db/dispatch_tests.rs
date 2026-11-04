use super::dispatch::{Dispatch, Effects, Event, OrderedEffects, Work, MAX_PENDING};
use super::fixtures::*;
use crate::commit::CommitResult;
use crate::identity::{ModelKey, RowRef};
use std::collections::BTreeSet;

fn event(revision: i64, model: &str) -> Event {
    Event { revision, models: BTreeSet::from([ModelKey::new(model)]), ..Event::default() }
}

#[test]
fn a_full_queue_merges_its_oldest_events_and_loses_no_dependency() {
    let dispatch = Dispatch::new();
    let total = MAX_PENDING + 40;
    for number in 0..total {
        dispatch.push(event(number as i64 + 1, &format!("model{number}")));
    }
    let Some(Work::Batch(batch)) = dispatch.next_work() else { panic!("a batch is queued") };
    assert!(batch.len() <= MAX_PENDING);
    let models: BTreeSet<ModelKey> = batch.iter().flat_map(|event| event.models.iter().cloned()).collect();
    assert_eq!(models.len(), total);
    assert_eq!(batch.iter().map(|event| event.revision).max(), Some(total as i64));
}

#[test]
fn merged_events_keep_replaced_pairs_and_reset_flags() {
    let mut first = event(1, "moments");
    first.replaced.push((rid("temp-1"), rid("m1")));
    let mut second = event(2, "users");
    second.reset_all = true;
    let merged = first.merge(second);
    assert_eq!(merged.revision, 2);
    assert!(merged.reset_all);
    assert_eq!(merged.replaced.len(), 1);
    assert_eq!(merged.models.len(), 2);
}

#[test]
fn an_event_of_a_commit_lists_models_rows_scopes_and_replaced_pairs() {
    let db = booted();
    let temp = db.create(&db.admit().unwrap(), "moments", obj(serde_json::json!({"text": "t"}))).unwrap();
    let mut envelope = crate::commit::CommitEnvelope::new(crate::commit::WriteOrigin::Response, db.generation());
    envelope.entity_ops.push(crate::commit::EntityOp::Destroy { model: ModelKey::new("moments"), id: temp.id.clone(), reason: crate::commit::DestroyReason::Replaced { by: rid("m7") } });
    envelope.entity_ops.push(crate::commit::EntityOp::Upsert { model: ModelKey::new("moments"), id: rid("m7"), row: obj(serde_json::json!({"id": "m7", "text": "t"})) });
    let result: CommitResult = db.shared.store.commit(envelope).unwrap();
    let event = Event::of_commit(&result);
    assert_eq!(event.revision, result.revision.unwrap());
    assert!(event.rows.contains(&RowRef::new(ModelKey::new("moments"), rid("m7"))));
    assert_eq!(event.replaced, vec![(temp.id.clone(), rid("m7"))]);
    assert_eq!(event.models, BTreeSet::from([ModelKey::new("moments")]));
}

#[test]
fn ordered_effects_run_in_push_order_and_a_push_from_an_effect_only_queues() {
    use std::sync::{Arc, Mutex};
    let ordered = Arc::new(OrderedEffects::default());
    let log = Arc::new(Mutex::new(Vec::new()));
    let note = |text: &'static str| {
        let log = log.clone();
        move || log.lock().unwrap().push(text)
    };
    let mut first = Effects::default();
    first.push({
        let (ordered, log, nested_note) = (ordered.clone(), log.clone(), note("nested"));
        move || {
            log.lock().unwrap().push("first starts");
            let mut nested = Effects::default();
            nested.push(nested_note);
            ordered.push(nested);
            ordered.run();
            log.lock().unwrap().push("first ends");
        }
    });
    let mut second = Effects::default();
    second.push(note("second"));
    ordered.push(first);
    ordered.push(second);
    ordered.run();
    assert_eq!(*log.lock().unwrap(), ["first starts", "first ends", "second", "nested"]);
    assert!(ordered.is_idle());
}

#[test]
fn a_panicking_effect_does_not_wedge_the_ordered_runner() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    let ordered = OrderedEffects::default();
    let mut failing = Effects::default();
    failing.push(|| panic!("effect failed"));
    ordered.push(failing);
    assert!(catch_unwind(AssertUnwindSafe(|| ordered.run())).is_err());
    assert!(ordered.is_idle(), "the runner is released after the unwind");
    let ran = Arc::new(AtomicBool::new(false));
    let mut next = Effects::default();
    next.push({
        let ran = ran.clone();
        move || ran.store(true, Ordering::SeqCst)
    });
    ordered.push(next);
    ordered.run();
    assert!(ran.load(Ordering::SeqCst), "the next batch runs");
}
