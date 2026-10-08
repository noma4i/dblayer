use super::*;
use std::collections::BTreeSet;
use crate::commit::WriteOrigin;
use crate::identity::RowId;
use crate::schema::RootShape;
use Effect::*;

fn set(effects: &[Effect]) -> BTreeSet<Effect> {
    effects.iter().copied().collect()
}

const PLAIN_DESTROY: [WriteOrigin; 5] = [WriteOrigin::Snapshot, WriteOrigin::Event, WriteOrigin::Patch, WriteOrigin::Optimistic, WriteOrigin::Response];

#[test]
fn effects_follow_origin_and_parent_authority() {
    assert_eq!(upsert_effects(WriteOrigin::Event), set(&[TouchParent]));
    assert_eq!(upsert_effects(WriteOrigin::Optimistic), set(&[TouchParent]));
    assert_eq!(upsert_effects(WriteOrigin::Replace), set(&[TouchParent]));
    assert!(upsert_effects(WriteOrigin::Snapshot).is_empty());
    assert!(upsert_effects(WriteOrigin::Patch).is_empty());
    assert!(upsert_effects(WriteOrigin::Response).is_empty());
    assert!(destroy_effects(WriteOrigin::Replace).is_empty());
    for origin in PLAIN_DESTROY {
        assert_eq!(destroy_effects(origin), set(&[CascadeDestroy]));
    }
    let all = set(&[TouchParent, CascadeDestroy]);
    assert_eq!(parent_authority_wins(true, &all), set(&[CascadeDestroy]));
    assert_eq!(parent_authority_wins(false, &all), all);
}

#[test]
fn replaced_ids_skip_plain_destroy() {
    let replaced = RowId::new("row-replaced").unwrap();
    let destroys = [
        (replaced.clone(), WriteOrigin::Replace),
        (RowId::new("row-event").unwrap(), WriteOrigin::Event),
        (RowId::new("row-optimistic").unwrap(), WriteOrigin::Optimistic),
    ];
    assert_eq!(replaced_ids(&destroys), [replaced].into());
}

#[test]
fn association_effects_follow_shape_origin_and_parent_plan() {
    assert_eq!(association_effects(RootShape::Upsert, WriteOrigin::Event, false), set(&[TouchParent]));
    assert!(association_effects(RootShape::Upsert, WriteOrigin::Snapshot, false).is_empty());
    assert!(association_effects(RootShape::Upsert, WriteOrigin::Patch, false).is_empty());
    assert_eq!(association_effects(RootShape::Upsert, WriteOrigin::Replace, false), set(&[TouchParent]));
    assert_eq!(association_effects(RootShape::Destroy, WriteOrigin::Event, false), set(&[CascadeDestroy]));
    assert!(association_effects(RootShape::Destroy, WriteOrigin::Replace, false).is_empty());
    assert!(association_effects(RootShape::Upsert, WriteOrigin::Event, true).is_empty());
    assert_eq!(association_effects(RootShape::Destroy, WriteOrigin::Event, true), set(&[CascadeDestroy]));
}

#[test]
fn replace_pairs_balance_and_cascade_follows_residency() {
    assert!(balanced_pair(RootShape::Upsert, WriteOrigin::Replace));
    assert!(balanced_pair(RootShape::Destroy, WriteOrigin::Replace));
    assert!(!balanced_pair(RootShape::Upsert, WriteOrigin::Event));
    assert!(!balanced_pair(RootShape::Destroy, WriteOrigin::Event));
    assert!(cascades(RootShape::Destroy, WriteOrigin::Event, true));
    assert!(!cascades(RootShape::Destroy, WriteOrigin::Event, false));
    assert!(!cascades(RootShape::Destroy, WriteOrigin::Replace, true));
    assert!(!cascades(RootShape::Destroy, WriteOrigin::Replace, false));
    assert!(touches_parent(RootShape::Upsert, WriteOrigin::Event, false));
    assert!(!touches_parent(RootShape::Upsert, WriteOrigin::Event, true));
}
