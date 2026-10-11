use super::*;
use crate::commit::{DestroyReason, EntityOp, WriteOrigin};
use crate::identity::{Generation, ModelKey, RowId, RowRef};

fn row(model: &str, id: &str) -> RowRef {
    RowRef::new(ModelKey::new(model), RowId::new(id).unwrap())
}

#[test]
fn cascade_destroys_children_then_resident_parent_once_each() {
    let parent = row("Parent", "parent-1");
    let [first, second, third] = [row("Child", "child-1"), row("Child", "child-2"), row("Child", "child-3")];
    let children = vec![first.clone(), second.clone(), third.clone()];
    assert_eq!(cascade_destroys(&parent, &children, true), [first.clone(), second.clone(), third, parent.clone()]);
    assert!(cascade_destroys(&parent, std::slice::from_ref(&first), false).is_empty());
    assert_eq!(cascade_destroys(&parent, &[first.clone(), second.clone(), first.clone()], true), [first, second, parent.clone()]);
    assert_eq!(cascade_destroys(&parent, std::slice::from_ref(&parent), true), [parent]);
}

#[test]
fn cascade_envelope_destroys_each_row_with_reason() {
    let destroys = vec![row("Child", "c1"), row("Parent", "p1")];
    let envelope = cascade_envelope(&destroys, DestroyReason::Local, WriteOrigin::Event, Generation::INITIAL);
    assert_eq!(envelope.entity_ops.len(), 2);
    assert!(matches!(&envelope.entity_ops[1], EntityOp::Destroy { reason: DestroyReason::Local, .. }));
    assert!(envelope.validate(Generation::INITIAL).is_ok());
}
