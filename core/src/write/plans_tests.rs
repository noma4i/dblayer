use super::*;

use crate::identity::{ModelKey, RowId, RowRef};

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
