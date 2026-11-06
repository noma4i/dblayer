use super::fixtures::{booted, rid, seed_tree};
use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, WriteOrigin};
use crate::identity::ModelKey;
use crate::owners::store::StoreOp;

#[test]
fn store_prop_cascade_no_orphans() {
    for source in ["remove_rows", "event", "local"] {
        let db = booted();
        seed_tree(&db);
        let before = db.revision_seq().unwrap();
        match source {
            "remove_rows" => db.write_queue_ops(&[StoreOp::RemoveRows { model: "users".into(), ids: vec!["u1".into()] }]).unwrap(),
            "event" => {
                let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
                envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new("users"), id: rid("u1"), reason: DestroyReason::Server });
                db.land(&db.admit().unwrap(), envelope).unwrap();
            }
            _ => { db.destroy(&db.admit().unwrap(), "users", &rid("u1")).unwrap(); }
        }
        assert_eq!(db.revision_seq().unwrap(), before + 1, "{source}");
        for (model, id) in [("users", "u1"), ("moments", "m1"), ("vibes", "v1"), ("vibes", "v2")] {
            assert!(db.find(model, &rid(id)).unwrap().is_none(), "{source}: {model}/{id}");
        }
        assert!(db.find("moments", &rid("m2")).unwrap().is_some(), "{source}");
    }
}
