use super::fixtures::*;
use super::*;
use crate::commit::{CommitEnvelope, OperationTransition, ScopeOp, ScopeRef, WriteOrigin};
use crate::operation::OperationStatus;
use crate::identity::{ModelKey, ListKey};
use crate::query::Query;
use crate::syncstate::{FailureKind, OperationFailure};
use crate::schema::{AssociationKind, AssociationSpec, FieldLink, FieldSpec, Coverage, PageDirection, RelationSpec, RemoteShape, RemoteSpec, VariableSource};
use crate::value::{relation_scope_key, ScalarKind};
use serde_json::json;

#[test]
fn create_stores_a_temp_row_and_enqueues_a_create_operation() {
    let db = booted();
    let row = db.create(&db.admit().unwrap(), "moments", obj(json!({"userId": "u1", "text": "hi"}))).unwrap();
    assert!(row.id.is_temp());
    assert_eq!(row.text("text"), Some("hi"));
    assert_eq!(db.find("moments", &row.id).unwrap().unwrap().text("text"), Some("hi"));
    let outbox = db.outbox().unwrap();
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].operation.action_key, "moments:create");
    assert_eq!(outbox[0].operation.temp_id, Some(row.id.clone()));
    assert_eq!(outbox[0].input["text"], json!("hi"));
    assert_eq!(outbox[0].input["id"], json!(row.id.as_str()));
}

#[test]
fn create_keeps_an_explicit_id_and_records_no_temp_id() {
    let db = booted();
    let row = db.create(&db.admit().unwrap(), "users", obj(json!({"id": "u7", "name": "Zed"}))).unwrap();
    assert_eq!(row.id.as_str(), "u7");
    assert_eq!(db.outbox().unwrap()[0].operation.temp_id, None);
}

#[test]
fn inv_cascade_03_create_touches_the_resident_parent() {
    let db = booted();
    put(&db, "chats", json!({"id": "c1", "title": "t"}));
    let before = db.find("chats", &rid("c1")).unwrap().unwrap().revision;
    db.create(&db.admit().unwrap(), "messages", obj(json!({"chatId": "c1", "body": "yo"}))).unwrap();
    let after = db.find("chats", &rid("c1")).unwrap().unwrap();
    assert!(after.revision > before);
    assert_eq!(after.text("title"), Some("t"));
}

const OLD: &str = "2026-01-01T00:00:00.000Z";
const NEW: &str = "2026-01-02T00:00:00.000Z";

fn chat_pointer(db: &Db) -> (Option<String>, Option<String>) {
    let chat = db.find("chats", &rid("c1")).unwrap().unwrap();
    (chat.text("lastMessageId").map(str::to_string), chat.text("lastMessageAt").map(str::to_string))
}

#[test]
fn create_sets_the_parent_fields_when_the_child_is_newer() {
    let db = booted();
    put(&db, "chats", json!({"id": "c1", "title": "t", "lastMessageId": "m0", "lastMessageAt": OLD}));
    let message = db.create(&db.admit().unwrap(), "messages", obj(json!({"chatId": "c1", "body": "yo", "createdAt": NEW}))).unwrap();
    assert_eq!(chat_pointer(&db), (Some(message.id.to_string()), Some(NEW.to_string())));
}

#[test]
fn create_keeps_the_pointer_of_a_pending_message_that_is_newer() {
    let db = booted();
    put(&db, "chats", json!({"id": "c1", "title": "t", "lastMessageId": "temp-9", "lastMessageAt": NEW}));
    db.create(&db.admit().unwrap(), "messages", obj(json!({"chatId": "c1", "body": "late", "createdAt": OLD}))).unwrap();
    assert_eq!(chat_pointer(&db), (Some("temp-9".to_string()), Some(NEW.to_string())));
}

#[test]
fn destroying_the_head_message_moves_the_pointer_to_the_previous_one_and_a_refusal_gives_it_back() {
    let db = booted();
    put(&db, "messages", json!({"id": "m1", "chatId": "c1", "createdAt": OLD}));
    put(&db, "messages", json!({"id": "m2", "chatId": "c1", "createdAt": NEW}));
    put(&db, "chats", json!({"id": "c1", "title": "t", "lastMessageId": "m2", "lastMessageAt": NEW}));
    db.destroy(&db.admit().unwrap(), "messages", &rid("m2")).unwrap();
    assert_eq!(chat_pointer(&db), (Some("m1".to_string()), Some(OLD.to_string())));
    let seq = db.outbox().unwrap()[0].operation.id.0;
    db.roll_back_at(&db.admit().unwrap(), seq, db.generation()).unwrap();
    assert_eq!(chat_pointer(&db), (Some("m2".to_string()), Some(NEW.to_string())));
}

#[test]
fn destroying_a_chat_does_not_bring_it_back_through_its_own_pointer() {
    let db = booted();
    put(&db, "messages", json!({"id": "m1", "chatId": "c1", "createdAt": OLD}));
    put(&db, "chats", json!({"id": "c1", "title": "t", "lastMessageId": "m1", "lastMessageAt": OLD}));
    db.destroy(&db.admit().unwrap(), "chats", &rid("c1")).unwrap();
    assert!(db.find("chats", &rid("c1")).unwrap().is_none());
}

#[test]
fn discarding_the_create_gives_the_parent_fields_back() {
    let db = booted();
    put(&db, "chats", json!({"id": "c1", "title": "t", "lastMessageId": "m0", "lastMessageAt": OLD}));
    let message = db.create(&db.admit().unwrap(), "messages", obj(json!({"chatId": "c1", "body": "yo", "createdAt": NEW}))).unwrap();
    let seq = db.outbox().unwrap()[0].operation.id.0;
    let mut envelope = CommitEnvelope::new(WriteOrigin::Response, db.generation());
    envelope.operation_transitions.push(OperationTransition::Fail { seq, status: OperationStatus::Failed, failure: OperationFailure::new(FailureKind::Rejected, "no") });
    db.shared.store.commit(envelope).unwrap();
    db.discard(&db.admit().unwrap(), "messages", &message.id, None).unwrap();
    assert_eq!(chat_pointer(&db), (Some("m0".to_string()), Some(OLD.to_string())));
}

#[test]
fn create_does_not_touch_a_missing_parent() {
    let db = booted();
    db.create(&db.admit().unwrap(), "messages", obj(json!({"chatId": "ghost", "body": "yo"}))).unwrap();
    assert!(db.find("chats", &rid("ghost")).unwrap().is_none());
}

#[test]
fn create_without_a_required_field_fails_and_writes_nothing() {
    let db = booted();
    assert!(matches!(db.create(&db.admit().unwrap(), "messages", obj(json!({"body": "yo"}))), Err(DbError::Schema(_))));
    assert_eq!(db.count(&Query::new("messages")).unwrap(), 0);
    assert!(db.outbox().unwrap().is_empty());
}

#[test]
fn update_merges_the_patch_into_the_stored_row() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    let row = db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "uno"}))).unwrap();
    assert_eq!(row.text("text"), Some("uno"));
    assert_eq!(row.text("userId"), Some("u1"));
    let stored = db.find("moments", &rid("m1")).unwrap().unwrap();
    assert_eq!(stored.text("text"), Some("uno"));
    assert_eq!(stored.text("userId"), Some("u1"));
}

#[test]
fn update_enqueues_an_update_operation_that_holds_the_values_it_replaces() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "uno"}))).unwrap();
    let outbox = db.outbox().unwrap();
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].operation.action_key, "moments:update");
    assert_eq!(outbox[0].operation.row_ids, vec![rid("m1")]);
    assert_eq!(outbox[0].input, json!({"id": "m1", "text": "uno"}));
    let held = &outbox[0].rollback.as_ref().unwrap()["rows"][0];
    assert_eq!((held["model"].as_str(), held["id"].as_str(), &held["before"]), (Some("moments"), Some("m1"), &json!({"text": "one"})));
    assert_eq!(held["owned"].as_object().unwrap().keys().collect::<Vec<_>>(), ["text"]);
}

#[test]
fn inv_cascade_03_update_touches_the_parent_of_the_row() {
    let db = booted();
    put(&db, "chats", json!({"id": "c1", "title": "t"}));
    put(&db, "messages", json!({"id": "g1", "chatId": "c1", "body": "a"}));
    let before = db.find("chats", &rid("c1")).unwrap().unwrap().revision;
    db.update(&db.admit().unwrap(), "messages", &rid("g1"), obj(json!({"body": "b"}))).unwrap();
    assert!(db.find("chats", &rid("c1")).unwrap().unwrap().revision > before);
}

#[test]
fn inv_row_05_update_with_unchanged_values_writes_nothing() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let delivered = calls(&log).len();
    let before = db.find("moments", &rid("m1")).unwrap().unwrap().revision;
    let row = db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "one"}))).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(row.revision, before);
    assert!(db.outbox().unwrap().is_empty());
    assert_eq!(calls(&log).len(), delivered);
}

#[test]
fn update_of_a_missing_row_fails() {
    let db = booted();
    assert!(matches!(db.update(&db.admit().unwrap(), "moments", &rid("nope"), obj(json!({"text": "x"}))), Err(DbError::NotFound { .. })));
    assert!(db.outbox().unwrap().is_empty());
}

#[test]
fn destroy_removes_the_row_and_enqueues_a_destroy_operation() {
    let db = booted();
    put(&db, "vibes", json!({"id": "v1", "momentId": "m1", "kind": "like"}));
    let destroyed = db.destroy(&db.admit().unwrap(), "vibes", &rid("v1")).unwrap();
    assert_eq!(destroyed.len(), 1);
    assert!(db.find("vibes", &rid("v1")).unwrap().is_none());
    let outbox = db.outbox().unwrap();
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].operation.action_key, "vibes:destroy");
    assert_eq!(outbox[0].input, json!({"id": "v1"}));
}

#[test]
fn inv_cascade_01_destroy_cascades_through_dependent_associations() {
    let db = booted();
    seed_tree(&db);
    let destroyed = db.destroy(&db.admit().unwrap(), "users", &rid("u1")).unwrap();
    assert_eq!(destroyed.len(), 4);
    assert!(db.find("users", &rid("u1")).unwrap().is_none());
    assert!(db.find("moments", &rid("m1")).unwrap().is_none());
    assert!(db.find("vibes", &rid("v1")).unwrap().is_none());
    assert!(db.find("vibes", &rid("v2")).unwrap().is_none());
    assert!(db.find("moments", &rid("m2")).unwrap().is_some());
    assert!(db.find("users", &rid("u2")).unwrap().is_some());
}

#[test]
fn inv_cascade_01_destroy_with_cascade_is_one_commit_and_one_operation() {
    let db = booted();
    seed_tree(&db);
    let before = db.revision_seq().unwrap();
    db.destroy(&db.admit().unwrap(), "users", &rid("u1")).unwrap();
    assert_eq!(db.revision_seq().unwrap(), before + 1);
    let outbox = db.outbox().unwrap();
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].operation.action_key, "users:destroy");
    let rolled = outbox[0].rollback.as_ref().unwrap()["rows"].as_array().unwrap().len();
    assert_eq!(rolled, 4);
}

#[test]
fn destroy_of_a_missing_row_fails() {
    let db = booted();
    assert!(matches!(db.destroy(&db.admit().unwrap(), "users", &rid("nope")), Err(DbError::NotFound { .. })));
    assert!(db.outbox().unwrap().is_empty());
}

fn threaded_db_remote() -> Option<RemoteSpec> {
    Some(thread_remote(Default::default()))
}

fn thread_remote(variables: std::collections::BTreeMap<String, VariableSource>) -> RemoteSpec {
    RemoteSpec {
        operation: "Thread".into(),
        shape: RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None },
        stale_time: "session".into(),
        empty_stale_time: None,
        selects_page_info: true,
        page_size: None,
        variables,
        root: None,
        total_from: None,
    }
}

fn threaded_db() -> Db {
    let remote = thread_remote;
    let mut spec = messages_spec();
    let mut thread = RelationSpec::new("thread", &["chatId"]);
    thread.remote = Some(remote(Default::default()));
    let mut media = RelationSpec::new("media", &["chatId"]);
    media.remote = Some(remote([("kinds".to_string(), VariableSource::Constant { value: json!(["image"]) })].into()));
    spec.relations.extend([thread, media]);
    Db::open_in_memory(vec![chats_spec(), spec], None).unwrap()
}

fn scope_of(relation: &str, chat: &str) -> ListKey {
    relation_scope_key("messages", relation, &[("chatId".to_string(), crate::value::relation_param_text(&json!(chat)))].into()).unwrap()
}

#[test]
fn create_lands_the_temp_row_in_the_unfiltered_scope_of_its_params() {
    let db = threaded_db();
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("messages").in_scope(scope_of("thread", "c1")), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let row = db.create(&db.admit().unwrap(), "messages", obj(json!({"chatId": "c1", "body": "yo"}))).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(ids(changes(&log).last().unwrap()), [row.id.to_string()]);
    let other = db.create(&db.admit().unwrap(), "messages", obj(json!({"chatId": "c2", "body": "elsewhere"}))).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(ids(changes(&log).last().unwrap()), [row.id.to_string()]);
    assert_eq!(db.shared.store.scope_members("messages", scope_of("media", "c1").as_str()).unwrap(), vec![]);
    assert!(db.shared.store.scope_members("messages", scope_of("thread", "c2").as_str()).unwrap().iter().any(|(id, _)| id == other.id.as_str()));
}

#[test]
fn changing_a_by_column_moves_the_row_between_the_unfiltered_scopes() {
    let db = threaded_db();
    put(&db, "chats", json!({"id": "c1", "title": "one"}));
    put(&db, "chats", json!({"id": "c2", "title": "two"}));
    let row = db.create(&db.admit().unwrap(), "messages", obj(json!({"chatId": "c1", "body": "yo"}))).unwrap();
    let (first, first_callback) = recorder();
    let (second, second_callback) = recorder();
    let _one = db.observe(Query::new("messages").in_scope(scope_of("thread", "c1")), first_callback).unwrap();
    let _two = db.observe(Query::new("messages").in_scope(scope_of("thread", "c2")), second_callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    db.update(&db.admit().unwrap(), "messages", &row.id, obj(json!({"chatId": "c2"}))).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(ids(changes(&first).last().unwrap()), Vec::<String>::new());
    assert_eq!(ids(changes(&second).last().unwrap()), [row.id.to_string()]);
}

#[test]
fn a_remote_relation_lists_its_scope_in_the_relation_order() {
    let mut spec = messages_spec();
    let mut thread = RelationSpec::new("thread", &["chatId"]);
    thread.remote = threaded_db_remote();
    thread.sort = crate::schema::Sort::Fields(vec![crate::schema::SortField { column: "body".into(), direction: crate::value::SortDirection::Asc }]);
    spec.relations.push(thread);
    let db = Db::open_in_memory(vec![chats_spec(), spec], None).unwrap();
    put(&db, "messages", json!({"id": "m1", "chatId": "c1", "body": "b"}));
    put(&db, "messages", json!({"id": "m2", "chatId": "c1", "body": "a"}));
    let scope = ScopeRef::new(ModelKey::new("messages"), scope_of("thread", "c1"));
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.scope_ops.extend([
        ScopeOp::Append { scope: scope.clone(), row: rid("m1"), order_key: "a".into() },
        ScopeOp::Append { scope, row: rid("m2"), order_key: "b".into() },
    ]);
    db.land(&db.admit().unwrap(), envelope).unwrap();
    let query = db.remote_relation_query("messages", "thread", &obj(json!({"chatId": "c1"}))).unwrap().unwrap();
    assert_eq!(db.query(&query).unwrap().iter().map(|row| row.id.to_string()).collect::<Vec<_>>(), ["m2", "m1"]);
}

#[test]
fn a_keyed_patch_upserts_and_removes_one_element_of_an_array_field() {
    let db = booted();
    put(&db, "messages", json!({"id": "m1", "chatId": "c1", "reactions": [{"userId": "u2", "body": "x"}]}));
    let react = |user: &str, body: &str| {
        let patch = obj(json!({"reactions": {"$keyed": {"by": "userId", "element": {"userId": {"$arg": "user"}, "body": {"$arg": "body"}}}}}));
        db.act(&db.admit().unwrap(), "messages", "react", &rid("m1"), patch, obj(json!({"user": user, "body": body}))).unwrap();
        db.find("messages", &rid("m1")).unwrap().unwrap().get("reactions").cloned().unwrap()
    };
    assert_eq!(react("u1", "a"), json!([{"userId": "u2", "body": "x"}, {"userId": "u1", "body": "a"}]));
    assert_eq!(react("u1", "b"), json!([{"userId": "u2", "body": "x"}, {"userId": "u1", "body": "b"}]));
    assert_eq!(react("u1", ""), json!([{"userId": "u2", "body": "x"}]));
}

fn reacting_db(linked: bool) -> Db {
    let mut messages = messages_spec();
    messages.fields.retain(|field| field.name != "reactions");
    messages.fields.push(FieldSpec::scalar("authorId", ScalarKind::Id).nullable().optional().indexed());
    messages.fields.push(FieldSpec {
        fields: vec![
            FieldSpec::scalar("body", ScalarKind::Str).nullable(),
            FieldSpec::scalar("userId", ScalarKind::Id).nullable().read_from("user.id"),
            FieldSpec {
            fields: vec![FieldSpec::scalar("id", ScalarKind::Id).nullable(), FieldSpec::scalar("name", ScalarKind::Str).nullable()],
            link: linked.then(|| FieldLink { model: "users".to_string(), id: "userId".to_string() }),
            ..FieldSpec::object("user").nullable()
        },
        ],
        ..FieldSpec::array("reactions").optional()
    });
    messages.associations.push(AssociationSpec::new("user", AssociationKind::BelongsTo, "users", "authorId"));
    let db = Db::open_in_memory(vec![users_spec(), moments_spec(), vibes_spec(), chats_spec(), messages], None).unwrap();
    put(&db, "messages", json!({"id": "m1", "chatId": "c1"}));
    db
}

fn react_as(db: &Db, user: &str) -> serde_json::Value {
    let patch = obj(json!({"reactions": {"$keyed": {"by": "userId", "element": {"userId": {"$arg": "user"}, "body": {"$arg": "body"}}}}}));
    db.act(&db.admit().unwrap(), "messages", "react", &rid("m1"), patch, obj(json!({"user": user, "body": "x"}))).unwrap();
    db.find("messages", &rid("m1")).unwrap().unwrap().get("reactions").cloned().unwrap()
}

#[test]
fn a_keyed_element_carries_the_nested_record_of_the_local_row_it_references() {
    let db = reacting_db(true);
    put(&db, "users", json!({"id": "u1", "name": "Ada"}));
    assert_eq!(react_as(&db, "u1"), json!([{"user.id": "u1", "body": "x", "user": {"id": "u1", "name": "Ada"}}]));
    put(&db, "messages", json!({"id": "m1", "chatId": "c1", "reactions": [{"user.id": "u1", "body": "x", "user": {"id": "u1", "name": "Ada Server"}}]}));
    let reactions = db.find("messages", &rid("m1")).unwrap().unwrap().get("reactions").cloned().unwrap();
    assert_eq!(reactions, json!([{"user.id": "u1", "body": "x", "user": {"id": "u1", "name": "Ada"}}]), "a read gives the row of the user, not the record the server wrote (D-76)");
}

#[test]
fn a_keyed_element_without_the_referenced_local_row_has_no_nested_record() {
    let db = reacting_db(true);
    assert_eq!(react_as(&db, "u9"), json!([{"user.id": "u9", "body": "x"}]));
}

#[test]
fn a_keyed_element_without_a_declared_link_has_no_nested_record_whatever_the_owner_associates() {
    let db = reacting_db(false);
    put(&db, "users", json!({"id": "u1", "name": "Ada"}));
    assert_eq!(react_as(&db, "u1"), json!([{"user.id": "u1", "body": "x"}]));
}

#[test]
fn create_args_reach_the_operation_input_and_stay_out_of_the_row() {
    let db = booted();
    let row = db.create_with(&db.admit().unwrap(), "moments", obj(json!({"userId": "u1", "text": "hi"})), obj(json!({"signedId": "blob-1"}))).unwrap();
    assert_eq!(db.outbox().unwrap()[0].input["signedId"], json!("blob-1"));
    assert_eq!(db.outbox().unwrap()[0].input["text"], json!("hi"));
    assert!(!db.find("moments", &row.id).unwrap().unwrap().data.contains_key("signedId"));
}

#[test]
fn update_args_alone_still_enqueue_an_operation_that_carries_them() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    db.update_with(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({})), obj(json!({"signedId": "blob-2"}))).unwrap();
    let outbox = db.outbox().unwrap();
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].input, json!({"id": "m1", "signedId": "blob-2"}));
}

#[test]
fn a_mapped_patch_value_follows_the_parameter_and_rolls_back_with_the_action() {
    let db = booted();
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    let patch = || obj(json!({"text": {"$map": {"from": "decision", "map": {"ACCEPT": "yes", "REJECT": "no"}}}}));
    db.act(&db.admit().unwrap(), "moments", "decide", &rid("m1"), patch(), obj(json!({"decision": "REJECT"}))).unwrap();
    assert_eq!(db.find("moments", &rid("m1")).unwrap().unwrap().text("text"), Some("no"));
    db.roll_back(&db.admit().unwrap(), db.outbox().unwrap()[0].operation.id.0).unwrap();
    assert_eq!(db.find("moments", &rid("m1")).unwrap().unwrap().text("text"), Some("one"));
    db.act(&db.admit().unwrap(), "moments", "decide", &rid("m1"), patch(), obj(json!({"decision": "ACCEPT"}))).unwrap();
    assert_eq!(db.find("moments", &rid("m1")).unwrap().unwrap().text("text"), Some("yes"));
}

fn authored_notes() -> Db {
    let remote_single = |operation: &str| crate::schema_fixtures::remote(operation, crate::schema::RemoteShape::Single, "5.minutes");
    let singleton = |name: &str| {
        let mut spec = crate::schema::ModelSpec::new(name, vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("balance", ScalarKind::Int)]);
        let mut current = crate::schema::RelationSpec::global("current");
        current.remote = Some(remote_single("Current"));
        spec.relations.push(current);
        spec
    };
    let mut notes = crate::schema::ModelSpec::new("notes", vec![
        FieldSpec::scalar("id", ScalarKind::Id),
        FieldSpec::scalar("roomId", ScalarKind::Id).indexed(),
        FieldSpec::scalar("authorId", ScalarKind::Id).optional(),
        FieldSpec::scalar("regionId", ScalarKind::Id).optional(),
    ]);
    let mut create = crate::schema::ActionDeclaration::new("create", crate::schema::RootShape::Upsert);
    create.mutation = Some("CreateNote".to_string());
    create.local = vec!["authorId".to_string(), "regionId".to_string()];
    for (field, model) in [("authorId", "wallets"), ("regionId", "regions")] {
        create.local_from.insert(field.to_string(), crate::schema::EffectSource::Row { model: model.to_string(), row: None, field: "id".to_string() });
    }
    notes.actions.push(create);
    Db::open_in_memory(vec![notes, singleton("wallets"), singleton("regions")], None).unwrap()
}

fn join_current(db: &Db, model: &str, id: &str) {
    let key = db.spec(model).unwrap().selection_key("current", &Default::default()).unwrap();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.scope_ops.push(ScopeOp::Append { scope: ScopeRef::new(ModelKey::new(model), key), row: crate::identity::RowId::new(id).unwrap(), order_key: "a".to_string() });
    db.land(&db.admit().unwrap(), envelope).unwrap();
}

#[test]
fn inv_dsl_09_a_create_fills_a_local_attribute_from_the_singleton_row_and_skips_it_without_one() {
    let db = authored_notes();
    let created = |arguments: serde_json::Value| {
        let row = db.create(&db.admit().unwrap(), "notes", obj(arguments)).unwrap();
        json!([row.get("authorId").cloned().filter(|value| !value.is_null()), row.get("regionId").cloned().filter(|value| !value.is_null())])
    };
    assert_eq!(created(json!({"roomId": "a"})), json!([null, null]));
    put(&db, "wallets", json!({"id": "w1", "balance": 1}));
    join_current(&db, "wallets", "w1");
    assert_eq!(created(json!({"roomId": "a"})), json!(["w1", null]));
    put(&db, "regions", json!({"id": "r1", "balance": 0}));
    join_current(&db, "regions", "r1");
    assert_eq!(created(json!({"roomId": "a"})), json!(["w1", "r1"]));
    assert_eq!(created(json!({"roomId": "a", "authorId": null, "regionId": null})), json!(["w1", "r1"]));
    assert_eq!(created(json!({"roomId": "a", "authorId": "w9", "regionId": "r9"})), json!(["w9", "r9"]));
}
