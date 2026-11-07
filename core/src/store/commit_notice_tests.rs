use super::fixtures::*;
use crate::identity::RowRef;
use crate::owners::contract::{CommittedBroadcast, Models};
use crate::store::{CommitResult, RowChange, RowChangeKind};
use std::collections::BTreeSet;

fn changed(model_name: &str, row_id: &str, kind: RowChangeKind) -> RowChange {
    RowChange { row: RowRef::new(model(model_name), id(row_id)), kind, fields: BTreeSet::new() }
}

fn result_of(rows: Vec<RowChange>) -> CommitResult {
    let mut result = CommitResult::noop(uuid::Uuid::nil());
    result.revision = Some(7);
    result.position = 3;
    result.rows = rows;
    result
}

fn pairs(rows: &[(&str, &str)]) -> BTreeSet<(String, String)> {
    rows.iter().map(|(table, row)| (table.to_string(), row.to_string())).collect()
}

type Kind<'a> = (&'a str, CommitResult, Vec<&'a str>, Vec<(&'a str, &'a str)>);

fn kinds() -> Vec<Kind<'static>> {
    let mut scoped = result_of(Vec::new());
    scoped.scopes.insert(scope("messages", "chat"));
    let mut member = result_of(Vec::new());
    member.members.insert(RowRef::new(model("items"), id("i1")));
    let mut renewed = result_of(vec![changed("messages", "m1", RowChangeKind::Updated)]);
    renewed.renewed.push(RowRef::new(model("items"), id("i9")));
    let mut replaced = result_of(Vec::new());
    replaced.replaced.push((RowRef::new(model("messages"), id("tmp")), id("m1")));
    let mut queued = result_of(Vec::new());
    queued.operations.insert(1);
    vec![
        ("row", result_of(vec![changed("messages", "m1", RowChangeKind::Inserted)]), vec!["row_messages"], vec![("row_messages", "m1")]),
        ("tombstone", result_of(vec![changed("messages", "m1", RowChangeKind::Destroyed)]), vec!["row_messages"], vec![("row_messages", "m1")]),
        ("cascade", result_of(vec![changed("items", "i1", RowChangeKind::Destroyed), changed("messages", "m1", RowChangeKind::Destroyed)]), vec!["row_items", "row_messages"], vec![("row_items", "i1"), ("row_messages", "m1")]),
        ("scope", scoped, vec!["row_messages"], vec![]),
        ("member", member, vec!["row_items"], vec![("row_items", "i1")]),
        ("renewed in another table", renewed, vec!["row_messages"], vec![("row_messages", "m1")]),
        ("replace", replaced, vec!["row_messages"], vec![("row_messages", "m1"), ("row_messages", "tmp")]),
        ("queue", queued, vec![crate::store::operation::QUEUE_TABLE], vec![]),
    ]
}

#[test]
fn store_prop_one_commit_notice() {
    for (kind, result, tables, rows) in kinds() {
        let notice = CommittedBroadcast::of_commit(&result);
        let named: BTreeSet<String> = notice.tables.iter().cloned().collect();
        assert_eq!((notice.revision, notice.position), (7, 3), "{kind}");
        assert_eq!(named, tables.iter().map(|table| table.to_string()).collect::<BTreeSet<_>>(), "{kind}: tables");
        assert_eq!(notice.rows, Some(pairs(&rows)), "{kind}: rows");
        assert!(notice.rows.iter().flatten().all(|(table, _)| named.contains(table)), "{kind}: a row names a table outside `tables`");
    }
}

#[test]
fn store_prop_one_commit_notice_reset_names_its_tables_and_no_rows() {
    let all = CommittedBroadcast::of_reset(5, None);
    assert_eq!((all.models, all.rows, all.tables), (Models::All, None, Vec::new()));
    let some = CommittedBroadcast::of_reset(5, Some(&["messages".to_string()]));
    assert_eq!((some.models, some.rows, some.tables), (Models::Set(["messages".to_string()].into()), None, vec!["row_messages".to_string()]));
}
