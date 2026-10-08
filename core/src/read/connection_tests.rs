use super::*;
use serde_json::json;

#[test]
fn nodes_prefer_the_nodes_array_then_edges_and_drop_nulls() {
    let nodes = json!({"nodes": ["a", null, "b"]});
    let edges = json!({"edges": [{"node": "a"}, {"other": "b"}, {"node": null}]});
    assert_eq!(connection_nodes(&nodes), [json!("a"), json!("b")]);
    assert_eq!(connection_nodes(&edges), [json!("a")]);
    assert!(connection_nodes(&json!({})).is_empty());
    assert!(connection_nodes(&json!("x")).is_empty());
}

#[test]
fn nodes_win_over_edges_when_both_exist() {
    let both = json!({"nodes": ["n"], "edges": [{"node": "e"}]});
    assert_eq!(connection_nodes(&both), [json!("n")]);
}

#[test]
fn page_info_reads_pagination_fields() {
    let page_info = connection_page_info(&json!({"pageInfo": {"hasNextPage": true, "endCursor": "c1"}}), None);
    assert!(page_info.has_next_page);
    assert_eq!(page_info.end_cursor.as_deref(), Some("c1"));
    let missing = connection_page_info(&json!({}), None);
    assert!(!missing.has_next_page);
    assert_eq!(missing.end_cursor, None);
}

#[test]
fn total_count_accepts_only_numbers() {
    assert_eq!(connection_total_count(&json!({"totalCount": 12.7})), Some(12));
    assert_eq!(connection_total_count(&json!({"totalCount": 9})), Some(9));
    assert_eq!(connection_total_count(&json!({"totalCount": "5"})), None);
    assert_eq!(connection_total_count(&json!({})), None);
}

#[test]
fn malformed_payloads_are_not_landable() {
    assert_eq!(landing_fault(&json!("x"), None), Some(LandingFault::PayloadNotObject));
    assert_eq!(landing_fault(&json!(null), None), Some(LandingFault::PayloadNotObject));
    assert_eq!(landing_fault(&json!({"nodes": []}), None), Some(LandingFault::MissingPageInfo));
    assert_eq!(landing_fault(&json!({"pageInfo": null}), None), Some(LandingFault::MissingPageInfo));
    assert_eq!(landing_fault(&json!({"pageInfo": {"hasNextPage": true}}), None), Some(LandingFault::CursorMissing));
    assert_eq!(landing_fault(&json!({"pageInfo": {"hasNextPage": true, "endCursor": null}}), None), Some(LandingFault::CursorMissing));
    assert_eq!(landing_fault(&json!({"pageInfo": {"hasNextPage": true, "endCursor": ""}}), None), Some(LandingFault::CursorMissing));
    assert!(!is_landable(&json!({"pageInfo": {"hasNextPage": true}}), None));
}

#[test]
fn next_page_with_cursor_and_last_page_are_landable() {
    assert_eq!(landing_fault(&json!({"pageInfo": {"hasNextPage": true, "endCursor": "c1"}}), None), None);
    assert_eq!(landing_fault(&json!({"pageInfo": {"hasNextPage": false}}), None), None);
    assert!(is_landable(&json!({"pageInfo": {"hasNextPage": false}}), None));
}
