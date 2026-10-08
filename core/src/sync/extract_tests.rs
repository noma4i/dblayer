use super::error::SyncError;
use super::extract::*;
use crate::schema::RemoteShape;
use crate::store::PageInfo;
use crate::schema::{Coverage, PageDirection};
use serde_json::json;

fn connection() -> RemoteShape {
    RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None }
}

#[test]
fn root_value_walks_dotted_path() {
    let data = json!({"viewer": {"feed": {"x": 1}}});
    assert_eq!(root_value(&data, "viewer.feed"), Some(&json!({"x": 1})));
    assert_eq!(root_value(&data, "viewer.missing"), None);
}

#[test]
fn single_returns_one_node_and_null_returns_none() {
    let extracted = extract(&json!({"me": {"id": "u1"}}), "me", &RemoteShape::Single).unwrap();
    assert_eq!(extracted.nodes.len(), 1);
    assert_eq!(extracted.page_info, None);
    assert!(extract(&json!({"me": null}), "me", &RemoteShape::Single).unwrap().nodes.is_empty());
}

#[test]
fn list_returns_object_items_only() {
    let data = json!({"items": [{"id": "a"}, null, {"id": "b"}]});
    let extracted = extract(&data, "items", &RemoteShape::List { coverage: Coverage::Complete }).unwrap();
    assert_eq!(extracted.nodes.len(), 2);
}

#[test]
fn list_without_array_is_malformed() {
    let result = extract(&json!({"items": 5}), "items", &RemoteShape::List { coverage: Coverage::Complete });
    assert!(matches!(result, Err(SyncError::Malformed(_))));
    let missing = extract(&json!({}), "items", &RemoteShape::List { coverage: Coverage::Complete });
    assert!(matches!(missing, Err(SyncError::Malformed(_))));
}

#[test]
fn connection_reads_edges_and_page_info() {
    let data = json!({"feed": {"edges": [{"node": {"id": "a"}}, {"node": {"id": "b"}}], "pageInfo": {"hasNextPage": true, "endCursor": "c2"}}});
    let extracted = extract(&data, "feed", &connection()).unwrap();
    assert_eq!(extracted.nodes.len(), 2);
    assert_eq!(extracted.page_info, Some(PageInfo { has_next_page: true, end_cursor: Some("c2".to_string()) }));
}

#[test]
fn connection_without_page_info_or_cursor_is_malformed() {
    let no_info = json!({"feed": {"nodes": [{"id": "a"}]}});
    assert!(matches!(extract(&no_info, "feed", &connection()), Err(SyncError::Malformed(_))));
    let no_cursor = json!({"feed": {"nodes": [], "pageInfo": {"hasNextPage": true}}});
    assert!(matches!(extract(&no_cursor, "feed", &connection()), Err(SyncError::Malformed(_))));
}

#[test]
fn custom_cursor_is_read_from_its_path_in_the_connection() {
    let cursor = crate::schema::CursorSpec { variable: "afterSequence".into(), from: "lastSequenceNumber".into(), numeric: true };
    let shape = RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: Some(cursor) };
    let data = json!({"feed": {"edges": [{"node": {"id": "m1"}}], "pageInfo": {"hasNextPage": true}, "lastSequenceNumber": 7}});

    let extracted = extract(&data, "feed", &shape).unwrap();

    assert_eq!(extracted.page_info, Some(PageInfo { has_next_page: true, end_cursor: Some("7".to_string()) }));
    let missing = json!({"feed": {"edges": [], "pageInfo": {"hasNextPage": true}}});
    assert!(extract(&missing, "feed", &shape).is_err());
}
