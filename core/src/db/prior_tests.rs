use super::*;
use super::fixtures::*;
use serde_json::json;

#[test]
fn corrupt_or_unreadable_prior_rejects_the_transaction() {
    for corrupt in [json!({"kind": "unknown", "model": "moments", "id": "m1"}), json!({"kind": "created", "model": "moments", "id": ""})] {
        let db = booted();
        put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "before"}));
        db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "after"}))).unwrap();
        let seq = db.outbox().unwrap()[0].operation.id.0;
        {
            let stored = db.shared.store.lock();
            stored.connection.execute("DELETE FROM op_effect_value WHERE seq = ?1", [seq]).unwrap();
            crate::nodes::write_tree(&stored.connection, "op_effect_value", &["seq", "position"], &[rusqlite::types::Value::Integer(seq), rusqlite::types::Value::Integer(0)], &corrupt).unwrap();
        }
        let entry = db.outbox().unwrap().remove(0);
        assert!(Prior::of(&entry).is_err());
        assert!(db.shared.read(&[], |reader| Ok(Prior::created_rows(reader, seq)?)).is_err());
        assert!(db.shared.read(&[], |reader| Ok(crate::store::merge::unconfirmed_fields(reader, None, "moments", "m1")?)).is_err());
        assert!(db.shared.read(&[], |reader| Ok(Prior::with_edits(json!({"rows": [corrupt.clone()]}), reader, &[])?)).is_err());
        assert!(db.roll_back(&db.admit().unwrap(), seq).is_err());
        assert_eq!(db.find("moments", &rid("m1")).unwrap().unwrap().text("text"), Some("after"));
        assert_eq!(db.outbox().unwrap().len(), 1);
        db.shared.store.lock().connection.execute_batch("DROP TABLE op_effect_value").unwrap();
        assert!(db.roll_back(&db.admit().unwrap(), seq).is_err());
    }
}
