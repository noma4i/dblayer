use serde_json::json;

use super::rig::*;
use super::{LoadOptions, OperationDef};
use crate::commit::{CommitEnvelope, EntityOp, ScopeOp, ScopeRef, WriteOrigin};
use crate::db::fixtures::{changes, ids, recorder};
use crate::db::{Db, WindowSpec};
use crate::identity::{ModelKey, RowId};
use crate::query::Query;
use crate::schema::{Coverage, PageDirection, RelationSpec, RemoteShape, Sort, SortField};
use crate::transport::{MockTransport, Reply};
use crate::value::SortDirection;

use super::SyncConfig;

fn thread_rig() -> Rig {
    let mut moments = moments_spec();
    let mut thread = RelationSpec::new("thread", &["userId"]);
    thread.remote = Some(remote("MomentsThread", RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: Some(2), cursor: None }, "1.minute"));
    thread.joins_events = true;
    thread.sort = Sort::Fields(vec![
        SortField { column: "text".to_string(), direction: SortDirection::AscNullsLast },
        SortField { column: "id".to_string(), direction: SortDirection::Asc },
    ]);
    moments.relations.push(thread);
    let db = Db::open_in_memory(vec![users_spec(), moments, vibes_spec()], None).unwrap();
    let rig = rig_on(db, MockTransport::new(), SyncConfig::default());
    rig.sync.register(OperationDef::new("MomentsThread", "operation MomentsThread { x }", "feed").paged(2));
    rig
}

fn node(number: usize) -> serde_json::Value {
    json!({"id": format!("m{number}"), "text": format!("t{number}"), "userId": "c1"})
}

fn append(rig: &Rig, number: usize, text: &str) {
    let scope = ScopeRef::new(ModelKey::new("moments"), rig.db.remote_relation_query("moments", "thread", &params(json!({"userId": "c1"}))).unwrap().unwrap().scope.unwrap());
    let id = RowId::new(&format!("m{number}")).unwrap();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, rig.db.generation());
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: id.clone(), row: obj(json!({"id": id.as_str(), "text": text, "userId": "c1"})) });
    envelope.scope_ops.push(ScopeOp::Append { scope, row: id, order_key: format!("k{number}") });
    rig.db.land(&rig.db.admit().unwrap(), envelope).unwrap();
    rig.db.await_revision(rig.db.revision_seq().unwrap());
}

fn obj(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    value.as_object().unwrap().clone()
}

async fn loaded_thread(rig: &Rig) -> Query {
    let reply = json!({"feed": {"nodes": (1..=7).map(node).collect::<Vec<_>>(), "pageInfo": {"hasNextPage": false, "endCursor": "c"}}});
    rig.transport.enqueue("MomentsThread", Reply::data(reply));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "thread", &params(json!({"userId": "c1"})), LoadOptions::default()).await.unwrap();
    rig.db.remote_relation_query("moments", "thread", &params(json!({"userId": "c1"}))).unwrap().unwrap()
}

#[tokio::test(start_paused = true)]
async fn a_window_over_the_thread_shape_follows_the_tail_and_ignores_rows_before_it() {
    let rig = thread_rig();
    let query = loaded_thread(&rig).await;
    assert!(query.scope.is_some(), "a relation that lists by a param column is a scoped query");
    let (log, callback) = recorder();
    let _window = rig.db.observe_window(query, WindowSpec::Tail { size: 3, max: None, paged: false }, callback).unwrap();
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    let first = changes(&log).pop().unwrap();
    assert_eq!(ids(&first), ["m5", "m6", "m7"]);
    assert_eq!((first.window.unwrap().has_before, first.window.unwrap().total), (true, Some(7)));

    append(&rig, 8, "t8");
    assert_eq!(ids(&changes(&log).pop().unwrap()), ["m6", "m7", "m8"]);

    put(&rig, "moments", json!({"id": "m9", "text": "t9", "userId": "c1"}));
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    let scope = ScopeRef::new(ModelKey::new("moments"), rig.db.remote_relation_query("moments", "thread", &params(json!({"userId": "c1"}))).unwrap().unwrap().scope.unwrap());
    let mut membership = CommitEnvelope::new(WriteOrigin::Event, rig.db.generation());
    membership.scope_ops.push(ScopeOp::Append { scope, row: RowId::new("m9").unwrap(), order_key: "k9".to_string() });
    rig.db.land(&rig.db.admit().unwrap(), membership).unwrap();
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    assert_eq!(ids(&changes(&log).pop().unwrap()), ["m7", "m8", "m9"], "a row that joins the scope alone enters the window");

    let (calls, loaded) = (changes(&log).len(), rig.db.counters().rows_loaded);
    put(&rig, "moments", json!({"id": "m7", "text": "t6x", "userId": "c1"}));
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    assert_eq!(changes(&log).len(), calls + 1);
    assert_eq!(rig.db.counters().rows_loaded - loaded, 1);

    let (calls, loaded) = (changes(&log).len(), rig.db.counters().rows_loaded);
    put(&rig, "moments", json!({"id": "m1", "text": "t1x", "userId": "c1"}));
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    assert_eq!(changes(&log).len(), calls, "a row before the window is not delivered");
    assert_eq!(rig.db.counters().rows_loaded, loaded);
}

#[tokio::test(start_paused = true)]
async fn a_scope_without_a_local_sort_is_refused_with_its_reason() {
    let rig = rig();
    let query = rig.db.remote_relation_query("moments", "by_user", &params(json!({"userId": "c1"}))).unwrap().unwrap();
    let refused = rig.db.observe_window(query, WindowSpec::Tail { size: 3, max: None, paged: false }, |_: &crate::db::Change| {});
    assert!(matches!(refused, Err(crate::db::DbError::Query(crate::query::QueryError::NotWindowable))));
}
