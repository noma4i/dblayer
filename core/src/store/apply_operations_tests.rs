fn record(seq: Option<i64>, status: OperationStatus, dedupe: Option<&str>) -> OperationRecord {
    OperationRecord {
        seq,
        action_key: "messages:send".into(),
        status,
        row_ids: vec![id("temp-1")],
        temp_id: Some(id("temp-1")),
        input: json!({"body": "hi"}),
        transport_started: false,
        base_revision: None,
        created_at: 5,
        dedupe_key: dedupe.map(str::to_string),
        rollback: Some(json!({"rows": []})),
        upload: None,
    }
}

fn transitions(store: &Store, items: Vec<OperationTransition>) -> crate::store::CommitResult {
    let mut envelope = envelope(store, vec![], vec![]);
    envelope.operation_transitions = items;
    store.commit(envelope).unwrap()
}

#[test]
fn begin_writes_operation_row_with_assigned_seq() {
    let (store, _) = booted_store();
    let result = transitions(&store, vec![OperationTransition::Begin(Box::new(record(None, OperationStatus::Pending, Some("k"))))]);
    assert_eq!(result.operations.len(), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op WHERE status = 'pending' AND dedupe_key = 'k'"), 1);
    assert_eq!(result.revision, Some(1));
}

#[test]
fn begin_with_existing_seq_replaces_the_row() {
    let (store, _) = booted_store();
    let first = transitions(&store, vec![OperationTransition::Begin(Box::new(record(None, OperationStatus::Pending, None)))]);
    let seq = *first.operations.iter().next().unwrap();
    transitions(&store, vec![OperationTransition::Begin(Box::new(record(Some(seq), OperationStatus::Failed, Some("k"))))]);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op WHERE dedupe_key IS NULL AND status = 'failed'"), 1);
}

#[test]
fn closing_an_operation_follows_its_status() {
    let cases = [
        ("committed", OperationStatus::Committed, "SELECT COUNT(*) FROM op", 0),
        ("failed", OperationStatus::Failed, "SELECT COUNT(*) FROM op WHERE status = 'failed' AND dedupe_key IS NULL", 1),
    ];
    for (case, status, operations_sql, operations) in cases {
        let (store, _) = booted_store();
        let started = transitions(&store, vec![OperationTransition::Begin(Box::new(record(None, OperationStatus::Pending, Some("k"))))]);
        let seq = *started.operations.iter().next().unwrap();
        let closing = if status.is_terminal() { OperationTransition::Close { seq, status } } else { OperationTransition::Fail { seq, status, failure: OperationFailure::new(FailureKind::Rejected, "no") } };
        transitions(&store, vec![closing]);
        assert_eq!(count(&store, operations_sql), operations, "{case}: operations");
    }
}

#[test]
fn operation_transitions_apply_only_from_the_statuses_the_machine_accepts() {
    let (store, _) = booted_store();
    let started = transitions(&store, vec![OperationTransition::Begin(Box::new(record(None, OperationStatus::Pending, Some("k"))))]);
    let seq = *started.operations.iter().next().unwrap();
    let rejected = || OperationFailure::new(FailureKind::Rejected, "no");
    let row = || text(&store, "SELECT status || ':' || transport_started || ':' || attempts FROM op").unwrap_or_default();
    let refused = |transition| transitions(&store, vec![transition]).unknown_operation_acks;
    assert_eq!(refused(OperationTransition::Postpone { seq, failure: rejected() }), 1);
    assert_eq!(refused(OperationTransition::Retry { seq }), 1);
    assert_eq!(refused(OperationTransition::Start { seq }), 0);
    assert_eq!(row(), "pending:1:1");
    assert_eq!(refused(OperationTransition::Start { seq }), 1);
    assert_eq!(refused(OperationTransition::Fail { seq, status: OperationStatus::Failed, failure: rejected() }), 0);
    assert_eq!(row(), "failed:1:1");
    assert_eq!(refused(OperationTransition::Retry { seq }), 0);
    assert_eq!(row(), "pending:0:0");
    let mut misuse = envelope(&store, vec![], vec![]);
    misuse.operation_transitions = vec![OperationTransition::Close { seq, status: OperationStatus::Failed }];
    assert!(matches!(store.commit(misuse), Err(StoreError::Constraint(_))));
    assert_eq!(row(), "pending:0:0");
    assert_eq!(refused(OperationTransition::Close { seq, status: OperationStatus::RolledBack }), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 0);
    assert_eq!(refused(OperationTransition::Close { seq, status: OperationStatus::RolledBack }), 1);
}

#[test]
fn closing_unknown_operation_counts_ack_and_still_applies_the_envelope() {
    let (store, _) = booted_store();
    let mut mixed = envelope(&store, vec![upsert("messages", message("m1", "c1"))], vec![]);
    mixed.operation_transitions = vec![OperationTransition::Close { seq: 99, status: OperationStatus::Committed }];
    let result = store.commit(mixed).unwrap();
    assert_eq!(result.unknown_operation_acks, 1);
    assert_eq!(store.counters().unknown_operation_acks, 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages"), 1);
}

#[test]
fn remove_respects_expected_status() {
    let (store, _) = booted_store();
    let started = transitions(&store, vec![OperationTransition::Begin(Box::new(record(None, OperationStatus::Pending, None)))]);
    let seq = *started.operations.iter().next().unwrap();
    let wrong = transitions(&store, vec![OperationTransition::Remove { seq, expected: Some(OperationStatus::Failed) }]);
    assert_eq!(wrong.unknown_operation_acks, 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 1);
    transitions(&store, vec![OperationTransition::Remove { seq, expected: Some(OperationStatus::Pending) }]);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 0);
}

#[test]
fn open_dedupe_key_is_unique_and_failed_frees_it() {
    let (store, _) = booted_store();
    let first = transitions(&store, vec![OperationTransition::Begin(Box::new(record(None, OperationStatus::Pending, Some("k"))))]);
    let mut duplicate = envelope(&store, vec![], vec![]);
    duplicate.operation_transitions = vec![OperationTransition::Begin(Box::new(record(None, OperationStatus::Pending, Some("k"))))];
    assert!(matches!(store.commit(duplicate), Err(StoreError::Constraint(_))));
    let seq = *first.operations.iter().next().unwrap();
    transitions(&store, vec![OperationTransition::Fail { seq, status: OperationStatus::Failed, failure: OperationFailure::new(FailureKind::Rejected, "no") }]);
    transitions(&store, vec![OperationTransition::Begin(Box::new(record(None, OperationStatus::Pending, Some("k"))))]);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 2);
}

#[test]
fn rows_and_operation_transition_commit_together() {
    let (store, _) = booted_store();
    let mut both = envelope(&store, vec![upsert("messages", message("temp-1", "c1"))], vec![]);
    both.operation_transitions = vec![OperationTransition::Begin(Box::new(record(None, OperationStatus::Pending, None)))];
    let result = store.commit(both).unwrap();
    assert_eq!(result.revision, Some(1));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op"), 1);
}

#[test]
fn direct_commit_keeps_tombstones() {
    let (store, clock) = booted_store();
    store.mark_tombstone("messages", "old", 1, 0).unwrap();
    clock.advance(std::time::Duration::from_millis(100_000_000));
    commit(&store, vec![destroy("messages", "fresh", DestroyReason::Server)], vec![]);
    assert!(store.is_tombstoned("messages", "old").unwrap());
    assert!(store.is_tombstoned("messages", "fresh").unwrap());
}

#[test]
fn a_replaced_destroy_is_reported_as_a_pair() {
    let (store, _) = booted_store();
    commit(&store, vec![upsert("messages", message("temp-1", "c1"))], vec![]);
    let result = commit(
        &store,
        vec![destroy("messages", "temp-1", DestroyReason::Replaced { by: id("m1") }), upsert("messages", message("m1", "c1"))],
        vec![],
    );
    assert_eq!(result.replaced, vec![(crate::identity::RowRef::new(model("messages"), id("temp-1")), id("m1"))]);
}

#[test]
fn the_commit_hook_runs_inside_the_commit_for_every_non_empty_result() {
    use crate::store::StoreEvent;
    use std::sync::{Arc, Mutex};
    let (store, _) = booted_store();
    let seen: Arc<Mutex<Vec<i64>>> = Arc::default();
    let sink = seen.clone();
    store.set_hook(Box::new(move |event| {
        if let StoreEvent::Committed(result) = event {
            sink.lock().unwrap().push(result.revision.unwrap());
        }
    }));
    commit(&store, vec![upsert("messages", message("m1", "c1"))], vec![]);
    commit(&store, vec![], vec![]);
    commit(&store, vec![upsert("messages", message("m1", "c1"))], vec![]);
    commit(&store, vec![upsert("messages", message("m2", "c1"))], vec![]);
    assert_eq!(*seen.lock().unwrap(), vec![1, 2]);
}

#[test]
fn the_commit_log_runs_while_every_read_is_excluded() {
    use crate::store::StoreEvent;
    use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};
    let (store, _) = booted_store();
    let store = Arc::new(store);
    let excluded = Arc::new(AtomicUsize::new(0));
    let (probe, count) = (store.clone(), excluded.clone());
    store.set_commit_log(Box::new(move |event| {
        if matches!(event, StoreEvent::Committed(_)) && probe.inner.try_lock().is_err() {
            count.fetch_add(1, Ordering::SeqCst);
        }
    }));
    commit(&store, vec![upsert("messages", message("m1", "c1"))], vec![]);
    assert_eq!(excluded.load(Ordering::SeqCst), 1);
}

fn sync_row(store: &Store, id: &str) -> Option<String> {
    text(store, &format!("SELECT state || ':' || attempts FROM row_sync WHERE model = 'messages' AND row_id = '{id}'"))
}

fn synced_rows(result: &crate::store::CommitResult) -> Vec<(String, RowChangeKind)> {
    result.rows.iter().map(|change| (change.row.id.to_string(), change.kind)).collect()
}

#[test]
fn begin_writes_row_sync_in_the_same_commit_and_reports_the_row_as_synced_when_it_has_no_entity_change() {
    let (store, _) = booted_store();
    let result = transitions(&store, vec![OperationTransition::Begin(Box::new(record(None, OperationStatus::Pending, None)))]);
    assert_eq!(sync_row(&store, "temp-1"), Some("pending:0".into()));
    assert_eq!(synced_rows(&result), [("temp-1".to_string(), RowChangeKind::Synced)]);
    assert!(result.scopes.is_empty());
}

#[test]
fn begin_with_an_entity_change_reports_the_row_once_with_its_own_kind() {
    let (store, _) = booted_store();
    let mut mixed = envelope(&store, vec![upsert("messages", message("temp-1", "c1"))], vec![]);
    mixed.operation_transitions = vec![OperationTransition::Begin(Box::new(record(None, OperationStatus::Pending, None)))];
    let result = store.commit(mixed).unwrap();
    assert_eq!(synced_rows(&result), [("temp-1".to_string(), RowChangeKind::Inserted)]);
    assert_eq!(sync_row(&store, "temp-1"), Some("pending:0".into()));
}

#[test]
fn inv_op_12_start_fail_and_retry_rewrite_row_sync_and_each_commit_reports_the_row() {
    use crate::value::sync::{FailureKind, OperationFailure};
    let (store, _) = booted_store();
    let begun = transitions(&store, vec![OperationTransition::Begin(Box::new(record(None, OperationStatus::Pending, None)))]);
    let seq = *begun.operations.iter().next().unwrap();
    let started = transitions(&store, vec![OperationTransition::Start { seq }]);
    assert_eq!((sync_row(&store, "temp-1"), synced_rows(&started)), (Some("sending:1".into()), vec![("temp-1".to_string(), RowChangeKind::Synced)]));
    let failure = OperationFailure::new(FailureKind::Server, "http 503");
    let failed = transitions(&store, vec![OperationTransition::Fail { seq, status: OperationStatus::Failed, failure: failure.clone() }]);
    assert_eq!((sync_row(&store, "temp-1"), synced_rows(&failed).len()), (Some("failed:1".into()), 1));
    assert_eq!(text(&store, "SELECT error_kind || ':' || error_message FROM row_sync WHERE row_id = 'temp-1'"), Some("server:http 503".to_string()));
    transitions(&store, vec![OperationTransition::Retry { seq }]);
    assert_eq!(sync_row(&store, "temp-1"), Some("pending:0".into()));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM op WHERE error_kind IS NULL AND transport_started = 0 AND status = 'pending'"), 1);
}

#[test]
fn start_only_applies_to_a_pending_operation_and_retry_only_to_a_stuck_one() {
    let (store, _) = booted_store();
    let begun = transitions(&store, vec![OperationTransition::Begin(Box::new(record(None, OperationStatus::Pending, None)))]);
    let seq = *begun.operations.iter().next().unwrap();
    let retried = transitions(&store, vec![OperationTransition::Retry { seq }]);
    assert!(retried.is_noop());
    transitions(&store, vec![OperationTransition::Start { seq }]);
    let twice = transitions(&store, vec![OperationTransition::Start { seq }]);
    assert!(twice.is_noop());
    assert_eq!(sync_row(&store, "temp-1"), Some("sending:1".into()));
    assert_eq!(store.counters().unknown_operation_acks, 2);
}

#[test]
fn closing_the_operation_removes_row_sync_and_reports_the_row() {
    let (store, _) = booted_store();
    let begun = transitions(&store, vec![OperationTransition::Begin(Box::new(record(None, OperationStatus::Pending, None)))]);
    let seq = *begun.operations.iter().next().unwrap();
    let closed = transitions(&store, vec![OperationTransition::Close { seq, status: OperationStatus::Committed }]);
    assert_eq!(sync_row(&store, "temp-1"), None);
    assert_eq!(synced_rows(&closed), [("temp-1".to_string(), RowChangeKind::Synced)]);
}

#[test]
fn retargeting_an_operation_moves_row_sync_to_the_new_id() {
    let (store, _) = booted_store();
    let begun = transitions(&store, vec![OperationTransition::Begin(Box::new(record(None, OperationStatus::Pending, None)))]);
    let seq = *begun.operations.iter().next().unwrap();
    let mut moved = record(Some(seq), OperationStatus::Pending, None);
    moved.row_ids = vec![id("m7")];
    moved.temp_id = None;
    let result = transitions(&store, vec![OperationTransition::Begin(Box::new(moved))]);
    assert_eq!((sync_row(&store, "temp-1"), sync_row(&store, "m7")), (None, Some("pending:0".into())));
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn row_sync_change_reaches_the_scopes_that_hold_the_row() {
    let (store, _) = booted_store();
    commit(&store, vec![upsert("messages", message("m1", "c1"))], vec![append(&scope("messages", "thread"), "m1", "a")]);
    let mut moved = record(None, OperationStatus::Pending, None);
    moved.row_ids = vec![id("m1")];
    moved.temp_id = None;
    let result = transitions(&store, vec![OperationTransition::Begin(Box::new(moved))]);
    assert_eq!(result.scopes, [scope("messages", "thread")].into());
}

#[test]
fn every_scalar_field_is_a_column_and_no_blob_holds_the_row() {
    let (store, _) = booted_store();
    let names: Vec<String> = {
        let inner = store.lock();
        let mut statement = inner.connection.prepare("SELECT name FROM pragma_table_info('row_messages')").unwrap();
        statement.query_map([], |row| row.get(0)).unwrap().map(Result::unwrap).collect()
    };
    for wanted in ["id", "revision", "born_rev", "chat_id", "body", "seen", "rev__chat_id", "rev__body", "rev__seen"] {
        assert!(names.iter().any(|name| name == wanted), "missing column {wanted} in {names:?}");
    }
    for gone in ["payload", "record_version", "field_rev"] {
        assert!(!names.iter().any(|name| name == gone), "column {gone} must not exist");
    }
}

#[test]
fn an_update_writes_only_the_changed_field_and_its_revision() {
    let (store, _) = booted_store();
    commit(&store, vec![upsert("messages", json!({"id": "m1", "chatId": "c1", "body": "b1"}))], vec![]);
    commit(&store, vec![upsert("messages", json!({"id": "m1", "chatId": "c1", "body": "b2"}))], vec![]);
    assert_eq!(text(&store, "SELECT body FROM row_messages WHERE id = 'm1'"), Some("b2".into()));
    assert_eq!(count(&store, "SELECT rev__body FROM row_messages WHERE id = 'm1'"), 2);
    assert_eq!(count(&store, "SELECT rev__chat_id FROM row_messages WHERE id = 'm1'"), 1);
    assert_eq!(count(&store, "SELECT revision FROM row_messages WHERE id = 'm1'"), 2);
}

#[test]
fn an_update_does_not_touch_the_columns_of_unchanged_fields() {
    let (store, _) = booted_store();
    commit(&store, vec![upsert("messages", json!({"id": "m1", "chatId": "c1", "body": "b1"}))], vec![]);
    exec(
        &store,
        "CREATE TEMP TABLE touched (column_name TEXT);
         CREATE TEMP TRIGGER touch_chat AFTER UPDATE OF chat_id ON row_messages BEGIN INSERT INTO touched VALUES ('chat_id'); END;
         CREATE TEMP TRIGGER touch_body AFTER UPDATE OF body ON row_messages BEGIN INSERT INTO touched VALUES ('body'); END;",
    );
    commit(&store, vec![upsert("messages", json!({"id": "m1", "chatId": "c1", "body": "b2"}))], vec![]);
    assert_eq!(text(&store, "SELECT group_concat(column_name) FROM touched"), Some("body".into()));
}

fn listed_messages_store() -> Store {
    let mut spec = messages_spec();
    let mut thread = crate::schema::RelationSpec::new("thread", &["chatId"]);
    thread.remote = Some(crate::schema::RemoteSpec {
        shape: crate::schema::RemoteShape::Single,
        operation: "Thread".into(),
        stale_time: "1.minute".into(),
        empty_stale_time: None,
        selects_page_info: false,
        page_size: None,
        variables: Default::default(),
        root: None,
        total_from: None,
    });
    spec.relations.push(thread);
    let (store, _) = booted_store();
    store.boot(&[spec, items_spec()], None).unwrap();
    store
}

#[test]
fn rows_joining_one_list_in_one_envelope_get_distinct_order_keys() {
    let store = listed_messages_store();
    commit(&store, vec![upsert("messages", message("m1", "c0")), upsert("messages", message("m2", "c0")), upsert("messages", message("m3", "c1"))], vec![]);
    commit(&store, vec![upsert("messages", message("m1", "c1")), upsert("messages", message("m2", "c1"))], vec![]);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM scope_member"), 2);
    assert_eq!(count(&store, "SELECT COUNT(DISTINCT order_key) FROM scope_member"), 2);
}

#[test]
fn appends_built_from_one_stale_snapshot_get_distinct_order_keys() {
    let (store, _) = booted_store();
    let thread = scope("messages", "thread");
    commit(&store, vec![], vec![append(&thread, "m1", "a0")]);
    commit(&store, vec![], vec![append(&thread, "m2", "a0"), append(&thread, "m3", "a0")]);
    let members = store.scope_members("messages", thread.key.as_str()).unwrap();
    assert_eq!(members.len(), 3);
    assert_eq!(members.iter().map(|member| member.1.clone()).collect::<HashSet<_>>().len(), 3);
}

#[test]
fn append_takes_the_key_a_move_in_the_same_envelope_vacates() {
    let (store, _) = booted_store();
    let thread = scope("messages", "thread");
    commit(&store, vec![], vec![append(&thread, "m1", "a0"), append(&thread, "m2", "a1")]);
    commit(&store, vec![], vec![append(&thread, "m3", "a0"), ScopeOp::Move { scope: thread.clone(), row: id("m1"), order_key: "a2".into() }]);
    assert_eq!(
        store.scope_members("messages", thread.key.as_str()).unwrap(),
        vec![("m3".into(), "a0".into()), ("m2".into(), "a1".into()), ("m1".into(), "a2".into())]
    );
}

static HEAD_LOOKUPS: AtomicUsize = AtomicUsize::new(0);

fn count_head_lookups(event: rusqlite::trace::TraceEvent<'_>) {
    if let rusqlite::trace::TraceEvent::Stmt(_, sql) = event {
        if sql.contains("SELECT MIN(order_key)") {
            HEAD_LOOKUPS.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[test]
fn joining_a_list_costs_one_head_lookup_per_row() {
    let store = listed_messages_store();
    let rows = |chat: &str| -> Vec<EntityOp> { (0..40).map(|index| upsert("messages", message(&format!("m{index}"), chat))).collect() };
    commit(&store, rows("c0"), vec![]);
    store.lock().connection.trace_v2(rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT, Some(count_head_lookups));
    commit(&store, rows("c1"), vec![]);
    assert_eq!(HEAD_LOOKUPS.load(Ordering::SeqCst), 40);
}
