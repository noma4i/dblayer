use serde_json::{json, Map, Value};

use super::normalize::normalize_nodes;
use crate::store::fixtures::all_specs;
use crate::store::Store;

fn nodes(value: Value) -> Vec<Map<String, Value>> {
    value.as_array().unwrap().iter().map(|node| node.as_object().unwrap().clone()).collect()
}

fn store() -> std::sync::Arc<Store> {
    Store::open_models(None, all_specs(), None, std::sync::Arc::new(crate::runtime::SystemClock)).map(std::sync::Arc::new).unwrap()
}

#[test]
fn data_belongs_to_null() {
    let db = store();
    let read = |model: &str, node: Value| normalize_nodes(&db, model, &nodes(json!([node]))).unwrap().roots.remove(0);
    assert_eq!(read("moments", json!({"id": "m1", "author": null})).get("userId"), Some(&Value::Null), "a null object clears the nullable key");
    assert_eq!(read("moments", json!({"id": "m1", "userId": null})).get("userId"), Some(&Value::Null), "a null id clears it");
    assert_eq!(read("moments", json!({"id": "m1", "text": "t"})).get("userId"), None, "no path keeps the key");
    assert_eq!(read("messages", json!({"id": "m1", "chat": null})).get("chatId"), None, "a required key without a default is left as it is");
}

#[test]
fn data_references_null() {
    let db = store();
    let read = |node: Value| normalize_nodes(&db, "users", &nodes(json!([node]))).unwrap();
    assert_eq!(read(json!({"id": "u1", "friends": null})).roots[0].get("friendIds"), Some(&json!([])), "null clears the list of ids");
    assert_eq!(read(json!({"id": "u1", "name": "Ann"})).roots[0].get("friendIds"), None, "no path keeps the list");
    let has_many = read(json!({"id": "u1", "moments": null}));
    assert_eq!((has_many.roots[0].contains_key("moments"), has_many.related.len()), (false, 0), "null on has_many changes nothing");
}
