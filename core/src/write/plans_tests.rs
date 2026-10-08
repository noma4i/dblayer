use super::*;
use crate::commit::{DestroyReason, EntityOp, WriteOrigin};
use crate::identity::{Generation, ModelKey, RowId, RowRef};
use crate::pacing::Counters;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};

fn row(model: &str, id: &str) -> RowRef {
    RowRef::new(ModelKey::new(model), RowId::new(id).unwrap())
}

fn values(items: &[(&str, Value)]) -> BTreeMap<Path, Value> {
    items.iter().map(|(name, value)| (Path::new(name), value.clone())).collect()
}

#[test]
fn cascade_destroys_children_then_resident_parent_once_each() {
    let parent = row("Parent", "parent-1");
    let [first, second, third] = [row("Child", "child-1"), row("Child", "child-2"), row("Child", "child-3")];
    let children = vec![first.clone(), second.clone(), third.clone()];
    assert_eq!(cascade_destroys(&parent, &children, true), [first.clone(), second.clone(), third, parent.clone()]);
    assert_eq!(cascade_destroy_count(&parent, &children, true), 4);
    assert!(cascade_destroys(&parent, &[first.clone()], false).is_empty());
    assert_eq!(cascade_destroy_count(&parent, &[first.clone()], false), 0);
    assert_eq!(cascade_destroys(&parent, &[first.clone(), second.clone(), first.clone()], true), [first, second, parent.clone()]);
    assert_eq!(cascade_destroys(&parent, &[parent.clone()], true), [parent]);
}

#[test]
fn foreign_key_filter_keeps_order_and_replace_origin_skips_cascade() {
    let parent_id = RowId::new("parent-1").unwrap();
    let other = RowId::new("parent-2").unwrap();
    let rows: Vec<(RowRef, RowId)> =
        (1..=4).map(|i| (row("Child", &format!("child-{i}")), if i % 2 == 0 { other.clone() } else { parent_id.clone() })).collect();
    assert_eq!(children_by_foreign_key(&rows, &parent_id), [rows[0].0.clone(), rows[2].0.clone()]);
    assert!(skips_cascade(true, WriteOrigin::Replace));
    assert!(!skips_cascade(true, WriteOrigin::Event));
    assert!(!skips_cascade(true, WriteOrigin::Snapshot));
    assert!(skips_cascade(false, WriteOrigin::Event));
}

#[test]
fn touch_merge_keeps_late_path_separates_parents_and_skips_empty_patch() {
    let (parent, other, empty) = (row("Post", "post-1"), row("Post", "post-2"), row("Post", "post-3"));
    let requests = [
        TouchRequest { parent: parent.clone(), patch: values(&[("title", json!("first"))]) },
        TouchRequest { parent: parent.clone(), patch: values(&[("title", json!("second")), ("body", json!("body"))]) },
        TouchRequest { parent: other.clone(), patch: values(&[("body", json!("other"))]) },
        TouchRequest { parent: empty.clone(), patch: BTreeMap::new() },
    ];
    let resident: HashSet<RowRef> = [parent.clone(), other.clone(), empty].into();
    let result = merge_touches(&requests, &resident);
    assert_eq!(result.len(), 2);
    assert_eq!(result[&parent], values(&[("title", json!("second")), ("body", json!("body"))]));
    assert_eq!(result[&parent].keys().map(Path::as_str).collect::<Vec<_>>(), ["body", "title"]);
    assert_eq!(result[&other], values(&[("body", json!("other"))]));
}

#[test]
fn non_resident_parents_are_dropped_and_counted() {
    let resident = row("Post", "post-1");
    let requests: Vec<TouchRequest> = ["post-1", "post-2", "post-3"]
        .iter()
        .map(|id| TouchRequest { parent: row("Post", id), patch: values(&[("title", json!(*id))]) })
        .collect();
    let set: HashSet<RowRef> = [resident.clone()].into();
    let result = merge_touches(&requests, &set);
    assert_eq!(result.len(), 1);
    assert_eq!(touch_drop_count(&requests, &set), 2);
    assert!(merge_touches(&requests, &HashSet::new()).is_empty());
    assert_eq!(touch_target(&RowId::new("post-1").unwrap(), &ModelKey::new("Post")), resident);
    let mut counters = Counters::default();
    count_touch_drops(&mut counters, &requests, &set);
    assert_eq!(counters.non_resident_touch_drops, 2);
}

#[test]
fn patch_envelope_builds_sorted_upserts_with_ids() {
    let (b, a) = (row("Post", "post-2"), row("Post", "post-1"));
    let patches = HashMap::from([(b, values(&[("count", json!(2))])), (a, values(&[("count", json!(1))]))]);
    let envelope = patch_envelope(&patches, WriteOrigin::Event, Generation::INITIAL);
    let ids: Vec<String> = envelope.entity_ops.iter().map(|op| op.row_ref().id.to_string()).collect();
    assert_eq!(ids, ["post-1", "post-2"]);
    match &envelope.entity_ops[0] {
        EntityOp::Upsert { row, .. } => assert_eq!(Value::Object(row.clone()), json!({"id": "post-1", "count": 1})),
        other => panic!("{other:?}"),
    }
    assert!(envelope.validate(Generation::INITIAL).is_ok());
    assert!(patch_envelope(&HashMap::new(), WriteOrigin::Event, Generation::INITIAL).is_empty());
}

#[test]
fn cascade_envelope_destroys_each_row_with_reason() {
    let destroys = vec![row("Child", "c1"), row("Parent", "p1")];
    let envelope = cascade_envelope(&destroys, DestroyReason::Local, WriteOrigin::Event, Generation::INITIAL);
    assert_eq!(envelope.entity_ops.len(), 2);
    assert!(matches!(&envelope.entity_ops[1], EntityOp::Destroy { reason: DestroyReason::Local, .. }));
    assert!(envelope.validate(Generation::INITIAL).is_ok());
}
