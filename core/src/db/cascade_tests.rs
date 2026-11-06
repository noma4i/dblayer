use super::fixtures::{booted, rid, seed_tree};
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
                db.land_events(&[("users".into(), r#"{"op":"delete","id":"u1"}"#.into())]).unwrap();
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
