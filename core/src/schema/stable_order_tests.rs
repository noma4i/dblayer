use super::*;
use crate::value::SortDirection;

fn remote() -> RemoteSpec {
    RemoteSpec {
        shape: RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None },
        operation: "Thread".into(),
        stale_time: "1.minute".into(),
        empty_stale_time: None,
        selects_page_info: true,
        page_size: None,
        variables: Default::default(),
        root: None,
        total_from: None,
    }
}

fn thread(name: &str) -> RelationSpec {
    let sort = Sort::Fields(vec![SortField { column: "sequenceNumber".into(), direction: SortDirection::AscNullsLast }]);
    RelationSpec { remote: Some(remote()), sort, order_stable: true, ..RelationSpec::new(name, &["chatId"]) }
}

fn host(relations: Vec<RelationSpec>) -> ModelSpec {
    let fields = vec![FieldSpec::raw("id"), FieldSpec::scalar("chatId", ScalarKind::Id).indexed(), FieldSpec::scalar("sequenceNumber", ScalarKind::Int).nullable()];
    ModelSpec { relations, ..ModelSpec::new("messages", fields) }
}

#[test]
fn a_stable_relation_needs_a_fetch_an_order_and_where_columns_without_text_params() {
    assert_eq!(host(vec![thread("thread")]).validate(&[]), Ok(()));
    let cases: [(&str, RelationSpec, &str); 4] = [
        ("fetch", RelationSpec { remote: None, ..thread("thread") }, "a fetch"),
        ("order", RelationSpec { sort: Sort::ServerOrder, ..thread("thread") }, "an order"),
        ("where", RelationSpec { by_columns: Vec::new(), is_global: true, ..thread("thread") }, "where columns and no text params"),
        ("text", RelationSpec { text_params: vec!["query".into()], ..thread("thread") }, "where columns and no text params"),
    ];
    for (name, relation, needs) in cases {
        assert_eq!(host(vec![relation]).validate(&[]), Err(DeclarationError::StableOrderNeeds { relation: "thread".into(), needs }), "{name}");
    }
}

#[test]
fn the_stable_order_of_a_scope_key_belongs_to_the_relation_that_made_it() {
    let model = host(vec![thread("thread"), RelationSpec { order_stable: false, ..thread("threads") }, thread("thread2")]);
    let key = |name: &str| model.selection_key(name, serde_json::json!({"chatId": "c1"}).as_object().unwrap()).unwrap();
    assert!(model.stable_order(&key("thread")).is_some());
    assert!(model.stable_order(&key("thread2")).is_some());
    assert!(model.stable_order(&key("threads")).is_none());
    assert!(model.stable_order(&ListKey::compose(&["messages", "other", "{}"])).is_none());
}
