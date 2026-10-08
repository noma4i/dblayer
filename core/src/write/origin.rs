use std::collections::{BTreeSet, HashSet};

use crate::commit::WriteOrigin;
use crate::identity::RowId;
use crate::schema::RootShape;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Effect {
    TouchParent,
    CascadeDestroy,
}

impl WriteOrigin {
    pub const ALL: [WriteOrigin; 6] = [
        WriteOrigin::Snapshot,
        WriteOrigin::Event,
        WriteOrigin::Response,
        WriteOrigin::Patch,
        WriteOrigin::Replace,
        WriteOrigin::Optimistic,
    ];
}

pub fn upsert_effects(origin: WriteOrigin) -> BTreeSet<Effect> {
    match origin {
        WriteOrigin::Event | WriteOrigin::Optimistic => [Effect::TouchParent].into(),
        WriteOrigin::Replace => [Effect::TouchParent].into(),
        WriteOrigin::Snapshot | WriteOrigin::Patch | WriteOrigin::Response => BTreeSet::new(),
    }
}

pub fn destroy_effects(origin: WriteOrigin) -> BTreeSet<Effect> {
    if origin == WriteOrigin::Replace {
        BTreeSet::new()
    } else {
        [Effect::CascadeDestroy].into()
    }
}

pub fn parent_authority_wins(parent_in_plan: bool, effects: &BTreeSet<Effect>) -> BTreeSet<Effect> {
    if !parent_in_plan {
        return effects.clone();
    }
    effects.iter().copied().filter(|effect| *effect == Effect::CascadeDestroy).collect()
}

pub fn replaced_ids(destroys: &[(RowId, WriteOrigin)]) -> HashSet<RowId> {
    destroys.iter().filter(|(_, origin)| *origin == WriteOrigin::Replace).map(|(row, _)| row.clone()).collect()
}

pub fn association_effects(shape: RootShape, origin: WriteOrigin, parent_in_plan: bool) -> BTreeSet<Effect> {
    let effects = match shape {
        RootShape::Upsert => upsert_effects(origin),
        RootShape::Destroy => destroy_effects(origin),
    };
    parent_authority_wins(parent_in_plan, &effects)
}

pub fn balanced_pair(_shape: RootShape, origin: WriteOrigin) -> bool {
    origin == WriteOrigin::Replace
}

pub fn touches_parent(shape: RootShape, origin: WriteOrigin, parent_in_plan: bool) -> bool {
    association_effects(shape, origin, parent_in_plan).contains(&Effect::TouchParent)
}

pub fn cascades(shape: RootShape, origin: WriteOrigin, parent_is_resident: bool) -> bool {
    association_effects(shape, origin, false).contains(&Effect::CascadeDestroy)
        && !super::plans::skips_cascade(parent_is_resident, origin)
}
