use super::*;
use crate::schema::{Coverage, PageDirection, RemoteShape, RemoteSpec};

fn remote(shape: RemoteShape) -> RemoteSpec {
    RemoteSpec { shape, operation: "Catalog".into(), stale_time: "catalog".into(), empty_stale_time: None, selects_page_info: false }
}

fn ids(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| item.to_string()).collect()
}

#[test]
fn only_complete_coverage_removes_missing_ids_from_the_first_page() {
    let page = coverage_change(Coverage::Page, &ids(&["a", "b"]), &ids(&["a"]));
    let complete = coverage_change(Coverage::Complete, &ids(&["a", "b"]), &ids(&["a"]));
    assert!(page.removed.is_empty());
    assert_eq!(complete.removed, ["b"]);
    assert!(replaces_membership(Coverage::Complete));
    assert!(!replaces_membership(Coverage::Page));
}

#[test]
fn drops_missing_needs_complete_coverage_and_first_page() {
    assert!(drops_missing(Coverage::Complete, true));
    assert!(!drops_missing(Coverage::Complete, false));
    assert!(!drops_missing(Coverage::Page, true));
}

#[test]
fn coverage_uses_the_remote_spec_or_defaults_to_page() {
    assert_eq!(coverage_of(Some(&remote(RemoteShape::List { coverage: Coverage::Complete }))), Coverage::Complete);
    let connection = remote(RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None });
    assert_eq!(coverage_of(Some(&connection)), Coverage::Page);
    assert_eq!(coverage_of(None), Coverage::Page);
}
