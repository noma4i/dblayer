use std::sync::{Arc, Mutex};

use serde_json::json;

use super::load_tests::rig;
use super::{EntityDelivery, EntityListener};
use crate::db::fixtures::put;

struct Removed(Mutex<Vec<String>>);

impl EntityListener for Removed {
    fn changed(&self, delivery: EntityDelivery) {
        self.0.lock().unwrap().extend(delivery.removed);
    }
}

#[tokio::test]
async fn inv_observe_27_a_leased_row_is_told_removed_before_logout_returns() {
    let (db, _) = rig().await;
    put(&db.db, "Moment", json!({"id": "m1", "text": "old"}));
    let listener = Arc::new(Removed(Mutex::new(Vec::new())));
    let live = db.live_entities("Moment".into(), listener.clone()).unwrap();
    live.retain(vec!["m1".into()], super::EntityFetch::None).unwrap();
    db.db.await_revision(db.db.revision_seq().unwrap());
    db.logout().await.unwrap();
    assert_eq!(listener.0.lock().unwrap().clone(), vec!["m1".to_string()]);
}
