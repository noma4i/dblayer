use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use crate::db::fixtures::*;
use crate::db::Db;
use crate::pacing::ManualClock;
use crate::query::Query;
use crate::schema::{AssociationKind, AssociationSpec, Dependent, FieldSpec, ModelSpec, Storage};
use crate::value::ScalarKind;

fn presence_with_pings() -> Vec<ModelSpec> {
    let mut presence = ModelSpec::new("presence", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("room", ScalarKind::Str).optional()]);
    presence.storage = Storage::Memory;
    presence.expire_after = Some("30.seconds".into());
    let mut pings = AssociationSpec::new("pings", AssociationKind::HasMany, "pings", "presenceId");
    pings.dependent = Some(Dependent::Destroy);
    presence.associations.push(pings);
    let pings = ModelSpec::new("pings", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("presenceId", ScalarKind::Id).indexed()]);
    vec![presence, pings]
}

#[test]
fn inv_cascade_02_an_expired_memory_row_takes_its_dependent_children_like_a_local_destroy() {
    let db = Db::open_in_memory(presence_with_pings(), None).unwrap();
    let clock = Arc::new(ManualClock::new(Duration::from_millis(1_000_000)));
    db.set_clock(clock.clone());
    put(&db, "presence", json!({"id": "p1", "room": "a"}));
    put(&db, "pings", json!({"id": "g1", "presenceId": "p1"}));
    put(&db, "pings", json!({"id": "g2", "presenceId": "p1"}));
    clock.advance(Duration::from_secs(31));
    db.expire_ephemeral().unwrap();
    assert_eq!(db.find("presence", &rid("p1")).unwrap(), None);
    assert_eq!(db.count(&Query::new("pings")).unwrap(), 0);
}
