use super::*;
use super::fixtures::*;
use serde_json::json;

#[test]
fn corrupt_or_unreadable_prior_rejects_the_transaction() {
    for corrupt in [json!({"rows": "broken"}), json!({"rows": [{"kind": "created", "model": "moments", "id": ""}]})] {
        let db = booted();
        put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "before"}));
        db.as_app().update("moments", &rid("m1"), obj(json!({"text": "after"}))).unwrap();
        let seq = db.outbox().unwrap()[0].operation.id.0;
        {
            let stored = db.shared.store.lock();
            crate::operation::write_value(&stored.connection, seq, "prior", Some(&corrupt)).unwrap();
        }
        let entry = db.outbox().unwrap().remove(0);
        assert!(Prior::of(&entry).is_err());
        assert!(db.shared.read(&[], |reader| Ok(Prior::created_rows(reader, seq)?)).is_err());
        assert!(db.shared.read(&[], |reader| Ok(Prior::unconfirmed_fields(reader, None, "moments", "m1")?)).is_err());
        assert!(db.shared.read(&[], |reader| Ok(Prior::with_edits(corrupt.clone(), reader, &[])?)).is_err());
        assert!(db.as_app().roll_back(seq).is_err());
        assert_eq!(db.find("moments", &rid("m1")).unwrap().unwrap().text("text"), Some("after"));
        assert_eq!(db.outbox().unwrap().len(), 1);
        db.shared.store.lock().connection.execute_batch("DROP TABLE op_value").unwrap();
        assert!(db.as_app().roll_back(seq).is_err());
    }
}
