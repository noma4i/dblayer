use std::collections::HashSet;

use crate::identity::RowRef;

pub fn cascade_destroys(parent: &RowRef, children: &[RowRef], parent_is_resident: bool) -> Vec<RowRef> {
    if !parent_is_resident {
        return Vec::new();
    }
    let mut seen = HashSet::new();
    let mut result: Vec<RowRef> = children.iter().filter(|child| *child != parent && seen.insert((*child).clone())).cloned().collect();
    result.push(parent.clone());
    result
}
