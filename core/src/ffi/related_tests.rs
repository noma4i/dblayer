use std::sync::Arc;

use super::stream::tape::{Seen, Tape, Watched};
use super::{Config, Database, LivePhase, StreamRequest, Transport};
use crate::schema::{AssociationKind, AssociationSpec, FieldSpec, ModelSpec};
use crate::value::ScalarKind;

fn id_field() -> FieldSpec { FieldSpec::scalar("id", ScalarKind::Id) }

async fn open() -> Arc<Database> {
    let mut user = ModelSpec::new("User", vec![id_field(), FieldSpec::scalar("name", ScalarKind::Str).optional()]);
    user.associations.push(AssociationSpec::new("moments", AssociationKind::HasMany, "Moment", "userId"));
    user.associations.push(AssociationSpec::new("profile", AssociationKind::HasOne, "Profile", "userId"));
    let mut moment = ModelSpec::new("Moment", vec![id_field(), FieldSpec::scalar("text", ScalarKind::Str), FieldSpec::scalar("userId", ScalarKind::Id).nullable().indexed(), FieldSpec::list("tagIds", ScalarKind::Id).nullable()]);
    moment.associations.push(AssociationSpec::new("author", AssociationKind::BelongsTo, "User", "userId"));
    moment.associations.push(AssociationSpec::new("tags", AssociationKind::References, "Tag", "tagIds"));
    let profile = ModelSpec::new("Profile", vec![id_field(), FieldSpec::scalar("userId", ScalarKind::Id).nullable().indexed()]);
    let tag = ModelSpec::new("Tag", vec![id_field(), FieldSpec::scalar("label", ScalarKind::Str).optional()]);
    Database::open(Config { path: None, data_version: None, cache_rows: 512 }, serde_json::to_string(&vec![user, moment, profile, tag]).unwrap(), None::<Arc<dyn Transport>>, None, None, None, None, None).await.unwrap()
}

fn settle(db: &Database) { db.db.await_revision(db.db.revision_seq().unwrap()); }

pub(super) fn ids(tape: &Tape) -> Vec<String> {
    let mut list: Vec<String> = Vec::new();
    for change in tape.heard().iter().filter(|change| change.phase.is_some() || !change.rows.is_empty() || change.full) {
        if change.full {
            list = change.rows.iter().map(|row| row.id.clone()).collect();
            continue;
        }
        for replaced in &change.replaced { if let Some(at) = list.iter().position(|id| *id == replaced.from) { list[at] = replaced.to.clone(); } }
        list.retain(|id| !change.removed.contains(id) && !change.placed.iter().any(|placed| placed.id == *id));
        let mut placed: Vec<_> = change.placed.iter().collect();
        placed.sort_by_key(|placed| placed.index);
        for placed in placed { list.insert(placed.index as usize, placed.id.clone()); }
    }
    list
}

fn last(tape: &Tape) -> Seen { tape.last().expect("a delivery") }

async fn make(db: &Database, model: &str, json: &str) { db.create(db.session_epoch(), model.into(), json.into(), "{}".into()).await.unwrap(); }

async fn patch(db: &Database, model: &str, id: &str, json: &str) { db.update(db.session_epoch(), model.into(), id.into(), json.into(), "{}".into()).await.unwrap(); }

async fn watch(db: &Database, query: &str, name: &str) -> (Arc<Tape>, Arc<Watched>) {
    let tape = Arc::new(Tape::default());
    let live = db.watch(StreamRequest::Related { query_json: query.into(), name: name.into() }, tape.clone());
    settle(db);
    (tape, live)
}

#[tokio::test]
async fn related_01_belongs_to_follows_parent_order_links_targets_and_late_arrivals() {
    let db = open().await;
    for (id, name) in [("u1", "Ann"), ("u2", "Bob")] { make(&db, "User", &format!(r#"{{"id":"{id}","name":"{name}"}}"#)).await; }
    for (id, text, user) in [("m1", "a", "\"u1\""), ("m2", "b", "\"u2\""), ("m3", "c", "\"u1\""), ("m4", "d", "null")] {
        make(&db, "Moment", &format!(r#"{{"id":"{id}","text":"{text}","userId":{user}}}"#)).await;
    }
    let (tape, live) = watch(&db, r#"{"model":"Moment","order":[{"field":"text","direction":"asc"}]}"#, "author").await;
    assert_eq!(ids(&tape), ["u1", "u2"], "parent order, one entry per target");

    patch(&db, "Moment", "m1", r#"{"userId":"u2"}"#).await;
    settle(&db);
    assert_eq!(ids(&tape), ["u2", "u1"], "a changed link moves the target");

    patch(&db, "User", "u2", r#"{"name":"Bobby"}"#).await;
    settle(&db);
    let update = last(&tape);
    assert_eq!((update.full, update.rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>()), (false, vec!["u2"]), "a changed target is one row");

    patch(&db, "Moment", "m4", r#"{"userId":"u9"}"#).await;
    settle(&db);
    assert_eq!(ids(&tape), ["u2", "u1"], "a missing target adds nothing");
    make(&db, "User", r#"{"id":"u9","name":"Late"}"#).await;
    settle(&db);
    assert_eq!(ids(&tape), ["u2", "u1", "u9"], "a target that arrives later joins at its parent position");

    db.destroy(db.session_epoch(), "Moment".into(), "m2".into()).await.unwrap();
    settle(&db);
    assert_eq!(ids(&tape), ["u2", "u1", "u9"], "m1 still points at u2");
    for gone in ["m1", "m3", "m4"] { db.destroy(db.session_epoch(), "Moment".into(), gone.into()).await.unwrap(); }
    settle(&db);
    assert!(ids(&tape).is_empty(), "no parents, no rows");
    live.cancel();
}

#[tokio::test]
async fn related_02_has_many_keeps_link_order_and_the_snapshot_has_no_target_total() {
    let db = open().await;
    for id in ["u1", "u2"] { make(&db, "User", &format!(r#"{{"id":"{id}","name":"n"}}"#)).await; }
    for (id, text, user) in [("m1", "a", "u2"), ("m2", "b", "u1"), ("m3", "c", "u2")] {
        make(&db, "Moment", &format!(r#"{{"id":"{id}","text":"{text}","userId":"{user}"}}"#)).await;
    }
    let (tape, live) = watch(&db, r#"{"model":"User","order":[{"field":"id","direction":"asc"}]}"#, "moments").await;
    assert_eq!(ids(&tape), ["m2", "m1", "m3"]);
    let shown = last(&tape);
    assert_eq!((shown.phase, shown.total, shown.size), (Some(LivePhase::Loaded), None, 3));
    live.cancel();
}

#[tokio::test]
async fn related_03_has_one_shows_the_first_child_and_the_next_one_after_it_goes() {
    let db = open().await;
    for id in ["u1", "u2"] { make(&db, "User", &format!(r#"{{"id":"{id}","name":"n"}}"#)).await; }
    for (id, user) in [("p1", "u2"), ("p2", "u1"), ("p3", "u1")] { make(&db, "Profile", &format!(r#"{{"id":"{id}","userId":"{user}"}}"#)).await; }
    let (tape, live) = watch(&db, r#"{"model":"User","order":[{"field":"id","direction":"asc"}]}"#, "profile").await;
    assert_eq!(ids(&tape), ["p2", "p1"]);
    db.destroy(db.session_epoch(), "Profile".into(), "p2".into()).await.unwrap();
    settle(&db);
    assert_eq!(ids(&tape), ["p3", "p1"]);
    live.cancel();
}

#[tokio::test]
async fn related_04_references_follow_the_array_and_a_shared_tag_shows_once() {
    let db = open().await;
    for id in ["t1", "t2", "t3"] { make(&db, "Tag", &format!(r#"{{"id":"{id}","label":"x"}}"#)).await; }
    make(&db, "Moment", r#"{"id":"m1","text":"a","tagIds":["t2","t1"]}"#).await;
    make(&db, "Moment", r#"{"id":"m2","text":"b","tagIds":["t1","t3"]}"#).await;
    let (tape, live) = watch(&db, r#"{"model":"Moment","order":[{"field":"text","direction":"asc"}]}"#, "tags").await;
    assert_eq!(ids(&tape), ["t2", "t1", "t3"]);
    patch(&db, "Moment", "m1", r#"{"tagIds":["t3"]}"#).await;
    settle(&db);
    assert_eq!(ids(&tape), ["t3", "t1"]);
    make(&db, "Tag", r#"{"id":"t9","label":"late"}"#).await;
    patch(&db, "Moment", "m2", r#"{"tagIds":["t1","t9"]}"#).await;
    settle(&db);
    assert_eq!(ids(&tape), ["t3", "t1", "t9"]);
    live.cancel();
}

struct Edit { read: usize, sent: usize, visited: usize, full: bool }

async fn edit_once(db: &Database, query: &str, name: &str, edit: impl std::future::Future<Output = ()>) -> Edit {
    let (tape, live) = watch(db, query, name).await;
    let counters = db.db.counters();
    edit.await;
    settle(db);
    let after = db.db.counters();
    let update = last(&tape);
    live.cancel();
    Edit { read: after.rows_loaded - counters.rows_loaded, sent: update.rows.len(), visited: after.related_visited - counters.related_visited, full: update.full }
}

async fn grid(count: usize) -> Arc<Database> {
    let db = open().await;
    for n in 0..count {
        make(&db, "User", &format!(r#"{{"id":"u{n:03}","name":"n"}}"#)).await;
        make(&db, "Moment", &format!(r#"{{"id":"m{n:03}","text":"t{n:03}","userId":"u{n:03}"}}"#)).await;
    }
    db
}

async fn family(count: usize) -> Arc<Database> {
    let db = open().await;
    make(&db, "User", r#"{"id":"u000","name":"n"}"#).await;
    make(&db, "User", r#"{"id":"u001","name":"n"}"#).await;
    for n in 0..count { make(&db, "Moment", &format!(r#"{{"id":"m{n:03}","text":"t{n:03}","userId":"u000"}}"#)).await; }
    db
}

#[tokio::test]
async fn related_05_one_edit_reads_sends_and_visits_by_the_touched_rows_for_every_source_shape() {
    let mut table: Vec<String> = Vec::new();
    for count in [20usize, 120] {
        let ordered = r#"{"model":"Moment","order":[{"field":"text","direction":"asc"}]}"#;
        let limited = format!(r#"{{"model":"Moment","order":[{{"field":"text","direction":"asc"}}],"limit":{count}}}"#);
        let pinned = r#"{"model":"User","where":{"type":"equal","field":"id","value":"u000"}}"#;
        let ranked = r#"{"model":"User","order":[{"field":"id","direction":"asc"}]}"#;
        let db = grid(count).await;
        let a = edit_once(&db, ordered, "author", patch(&db, "User", "u010", r#"{"name":"changed"}"#)).await;
        let db = grid(count).await;
        let d = edit_once(&db, &limited, "author", patch(&db, "User", "u011", r#"{"name":"changed"}"#)).await;
        let db = family(count).await;
        let b = edit_once(&db, pinned, "moments", patch(&db, "Moment", "m010", r#"{"text":"changed"}"#)).await;
        let db = family(count).await;
        let c = edit_once(&db, ranked, "moments", patch(&db, "Moment", "m011", r#"{"text":"changed"}"#)).await;
        for (shape, found, visited) in [("a ranked 1 target", &a, 2), ("b where id", &b, count + 1), ("c ranked has_many", &c, count + 1), ("d limit", &d, 2)] {
            table.push(format!("{count} {shape}: read {} sent {} visited {}", found.read, found.sent, found.visited));
            assert_eq!((found.full, found.read, found.sent, found.visited), (false, 2, 1, visited), "{shape} at {count}: {table:#?}");
        }
    }
    println!("{}", table.join("\n"));
}

struct Canceller {
    live: std::sync::Mutex<Option<Arc<Watched>>>,
    cancelled: std::sync::mpsc::Sender<()>,
}

impl crate::ffi::stream::tape::Reader for Canceller {
    fn delivered(&self, _: super::StreamDelivery) {
        let Some(live) = self.live.lock().unwrap().take() else { return };
        live.cancel();
        self.cancelled.send(()).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn related_cancel_inside_callback() {
    let census = |db: &Database| { let census = db.subscription_census(); (census.observed_queries, census.subscriptions) };
    for link_change in [true, false] {
        let db = open().await;
        for (id, name) in [("u1", "Ann"), ("u2", "Bob")] { make(&db, "User", &format!(r#"{{"id":"{id}","name":"{name}"}}"#)).await; }
        for (id, user) in [("m1", "u1"), ("m2", "u2")] { make(&db, "Moment", &format!(r#"{{"id":"{id}","text":"{id}","userId":"{user}"}}"#)).await; }
        let base = census(&db);
        let (cancelled, done) = std::sync::mpsc::channel();
        let listener = Arc::new(Canceller { live: std::sync::Mutex::new(None), cancelled });
        let query = r#"{"model":"Moment","order":[{"field":"text","direction":"asc"}]}"#;
        let live = db.watch(StreamRequest::Related { query_json: query.into(), name: "author".into() }, listener.clone());
        settle(&db);
        *listener.live.lock().unwrap() = Some(live.clone());
        if link_change { patch(&db, "Moment", "m1", r#"{"userId":"u2"}"#).await; } else { patch(&db, "User", "u1", r#"{"name":"Anna"}"#).await; }
        crate::test_wait::recv_within(&done);
        drop(live);
        crate::test_wait::yield_until("the census did not return to its base", || census(&db) == base).await;
        assert_eq!(db.stream_census().open, 0, "link_change={link_change}");
    }
}
