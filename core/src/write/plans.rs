use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::{Map, Value};

use super::path::Path;
use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, WriteOrigin};
use crate::identity::{Generation, ModelKey, RowId, RowRef};
use crate::pacing::Counters;

pub type PathValues = BTreeMap<Path, Value>;

#[derive(Debug, Clone, PartialEq)]
pub struct TouchRequest {
    pub parent: RowRef,
    pub patch: PathValues,
}

pub fn cascade_destroys(parent: &RowRef, children: &[RowRef], parent_is_resident: bool) -> Vec<RowRef> {
    if !parent_is_resident {
        return Vec::new();
    }
    let mut seen = HashSet::new();
    let mut result: Vec<RowRef> = children.iter().filter(|child| *child != parent && seen.insert((*child).clone())).cloned().collect();
    result.push(parent.clone());
    result
}

pub fn cascade_destroy_count(parent: &RowRef, children: &[RowRef], parent_is_resident: bool) -> usize {
    cascade_destroys(parent, children, parent_is_resident).len()
}

pub fn children_by_foreign_key(rows: &[(RowRef, RowId)], parent_id: &RowId) -> Vec<RowRef> {
    rows.iter().filter(|(_, foreign_key)| foreign_key == parent_id).map(|(row, _)| row.clone()).collect()
}

pub fn skips_cascade(parent_is_resident: bool, origin: WriteOrigin) -> bool {
    !parent_is_resident || origin == WriteOrigin::Replace
}

pub fn merge_touches(requests: &[TouchRequest], resident_parents: &HashSet<RowRef>) -> HashMap<RowRef, PathValues> {
    let mut result: HashMap<RowRef, PathValues> = HashMap::new();
    for request in requests.iter().filter(|request| resident_parents.contains(&request.parent) && !request.patch.is_empty()) {
        result.entry(request.parent.clone()).or_default().extend(request.patch.clone());
    }
    result
}

pub fn touch_drop_count(requests: &[TouchRequest], resident_parents: &HashSet<RowRef>) -> usize {
    requests.iter().filter(|request| !resident_parents.contains(&request.parent)).count()
}

pub fn count_touch_drops(counters: &mut Counters, requests: &[TouchRequest], resident_parents: &HashSet<RowRef>) {
    counters.non_resident_touch_drops += touch_drop_count(requests, resident_parents);
}

pub fn touch_target(child_foreign_key: &RowId, model: &ModelKey) -> RowRef {
    RowRef::new(model.clone(), child_foreign_key.clone())
}

pub fn patch_envelope(patches: &HashMap<RowRef, PathValues>, origin: WriteOrigin, generation: Generation) -> CommitEnvelope {
    let mut parents: Vec<&RowRef> = patches.keys().collect();
    parents.sort_by(|a, b| (a.model.as_str(), a.id.as_str()).cmp(&(b.model.as_str(), b.id.as_str())));
    let mut envelope = CommitEnvelope::new(origin, generation);
    for parent in parents {
        let mut row = Map::new();
        row.insert("id".to_string(), Value::String(parent.id.to_string()));
        row.extend(patches[parent].iter().map(|(path, value)| (path.as_str().to_string(), value.clone())));
        envelope.entity_ops.push(EntityOp::Upsert { model: parent.model.clone(), id: parent.id.clone(), row });
    }
    envelope
}

pub fn cascade_envelope(destroys: &[RowRef], reason: DestroyReason, origin: WriteOrigin, generation: Generation) -> CommitEnvelope {
    let mut envelope = CommitEnvelope::new(origin, generation);
    envelope.entity_ops = destroys
        .iter()
        .map(|row| EntityOp::Destroy { model: row.model.clone(), id: row.id.clone(), reason: reason.clone() })
        .collect();
    envelope
}
