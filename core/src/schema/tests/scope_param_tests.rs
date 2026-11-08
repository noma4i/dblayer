//! P-DATA-73: a parameter of a selection that has no `where` is typed JSON; a text for a number or a bool is refused before the request, and the key of the window keeps the type.

use serde_json::{json, Value};

use crate::schema_fixtures::moments_spec;

fn key(relation: &str, params: Value) -> Result<String, crate::schema::SelectionKeyError> {
    moments_spec().selection_key(relation, params.as_object().unwrap()).map(|key| key.as_str().to_string())
}

#[test]
fn data_scope_param_typed() {
    assert!(key("since", json!({"afterSequence": 5, "pinned": true})).is_ok(), "int and bool are numbers and bools");
    for (case, params) in [
        ("a text for an int", json!({"afterSequence": "5", "pinned": true})),
        ("a text for a bool", json!({"afterSequence": 5, "pinned": "true"})),
        ("a bool for an int", json!({"afterSequence": true, "pinned": true})),
        ("a number for a bool", json!({"afterSequence": 5, "pinned": 1})),
    ] {
        assert!(key("since", params).is_err(), "{case} is refused");
    }
    let (null, empty, word) = (key("vibe_feed", json!({"vibeId": null})).unwrap(), key("vibe_feed", json!({"vibeId": ""})).unwrap(), key("vibe_feed", json!({"vibeId": "null"})).unwrap());
    assert!(null != empty && null != word && empty != word, "null and a text are different windows");
}
