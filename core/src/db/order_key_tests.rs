use super::fixtures::*;
use super::*;
use crate::commit::ScopeRef;
use crate::identity::{ModelKey, ScopeKey};
use crate::schema::Coverage;
use serde_json::{json, Value};

fn rows(ids: &[usize]) -> Vec<serde_json::Map<String, Value>> {
    ids.iter().map(|id| obj(json!({"id": format!("m{id}"), "userId": "u1", "text": "t"}))).collect()
}

#[test]
fn consecutive_refreshes_never_hand_two_members_one_order_key() {
    let db = booted();
    let scope = ScopeRef::new(ModelKey::new("moments"), ScopeKey::from_raw("feed"));
    let steps: [(&[usize], Coverage); 4] = [
        (&[3, 0, 6, 2], Coverage::Complete),
        (&[2, 3, 4, 5, 7], Coverage::Page),
        (&[0, 5, 2], Coverage::Page),
        (&[7, 6, 1, 3, 2], Coverage::Complete),
    ];
    for (ids, coverage) in steps {
        db.replace(&ReplaceRequest::new(scope.clone(), coverage), rows(ids)).unwrap();
    }
    let stored = db.shared.store.scope_members("moments", "feed").unwrap();
    let order: Vec<&str> = stored.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(order, ["m7", "m6", "m1", "m3", "m2"]);
}
