use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Map, Value};

use crate::db::Db;
use crate::pacing::ManualClock;
use crate::schema::{
    ActionDeclaration, AssociationKind, AssociationSpec, Coverage, FieldSpec, ModelSpec, OnFailure, PageDirection, RelationSpec,
    RemoteShape, RemoteSpec, RootShape, UpdatesSpec, EphemeralSpec,
};
use crate::transport::MockTransport;
use crate::value::ScalarKind;

use super::{OperationDef, Sync, SyncConfig};

pub const START_MS: u64 = 1_000_000;

fn remote(operation: &str, shape: RemoteShape, stale: &str) -> RemoteSpec {
    let selects_page_info = matches!(shape, RemoteShape::Connection { .. });
    RemoteSpec { operation: operation.to_string(), shape, stale_time: stale.to_string(), empty_stale_time: None, selects_page_info, updates: None, page_size: None }
}

pub fn users_spec() -> ModelSpec {
    let mut spec = ModelSpec::new("users", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("name", ScalarKind::Str).optional()]);
    spec.ephemeral.push(EphemeralSpec::new(FieldSpec::scalar("typing", ScalarKind::Bool).with_default(json!(false))).resetting_after("5.seconds"));
    let mut current = RelationSpec::global("current");
    current.remote = Some(remote("CurrentUser", RemoteShape::Single, "5.minutes"));
    spec.relations.push(current);
    spec
}

pub fn moments_spec() -> ModelSpec {
    let mut spec = ModelSpec::new(
        "moments",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("userId", ScalarKind::Id).nullable().indexed(),
            FieldSpec::scalar("text", ScalarKind::Str).optional(),
            FieldSpec::scalar("likedByMe", ScalarKind::Bool).optional(),
        ],
    );
    spec.associations.push(AssociationSpec::new("author", AssociationKind::BelongsTo, "users", "userId"));
    let forward = RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: Some(2), cursor: None };
    let backward = RemoteShape::Connection { direction: PageDirection::Backward, coverage: Coverage::Page, max_pages: None, cursor: None };
    let mut feed = RelationSpec::global("feed");
    feed.remote = Some(remote("MomentsFeed", forward, "5.minutes"));
    let mut history = RelationSpec::global("history");
    history.remote = Some(remote("MomentsHistory", backward, "5.minutes"));
    let mut by_user = RelationSpec::new("by_user", &["userId"]);
    by_user.remote = Some(remote("MomentsByUser", RemoteShape::List { coverage: Coverage::Complete }, "1.minute"));
    let mut empty_aware = RelationSpec::global("empty_aware");
    let mut spec_remote = remote("MomentsByUser", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes");
    spec_remote.empty_stale_time = Some("10.seconds".to_string());
    empty_aware.remote = Some(spec_remote);
    let mut pulse = RelationSpec::global("pulse");
    let mut pulse_remote = remote("MomentsPulse", RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: Some(2), cursor: None }, "5.minutes");
    pulse_remote.updates = Some(UpdatesSpec { subscription: "MomentEvents".to_string(), deleted_by: Some("destroyed".to_string()), set: Map::new() });
    pulse.remote = Some(pulse_remote);
    spec.relations.extend([feed, history, by_user, empty_aware, pulse, RelationSpec::global("recent")]);
    spec.actions = vec![
        bound("create", RootShape::Upsert, "CreateMoment"),
        bound("update", RootShape::Upsert, "UpdateMoment"),
        bound("destroy", RootShape::Destroy, "DeleteMoment"),
        like_action(),
    ];
    spec
}

fn bound(name: &str, shape: RootShape, mutation: &str) -> ActionDeclaration {
    let mut action = ActionDeclaration::new(name, shape);
    action.mutation = Some(mutation.to_string());
    action.has_optimistic = name == "create";
    action.has_temp_row_retention = name == "create";
    if name == "create" {
        action.variables.insert("text".to_string(), "text".to_string());
    }
    action
}

fn like_action() -> ActionDeclaration {
    let mut action = bound("like", RootShape::Upsert, "LikeMoment");
    action.variables.insert("momentId".to_string(), "id".to_string());
    action.optimistic.insert("likedByMe".to_string(), json!(true));
    action.refetch.push("history".to_string());
    action
}

pub fn vibes_spec() -> ModelSpec {
    ModelSpec::new("vibes", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("kind", ScalarKind::Str).optional()])
}

pub fn specs() -> Vec<ModelSpec> {
    vec![users_spec(), moments_spec(), vibes_spec()]
}

pub fn keeping_specs() -> Vec<ModelSpec> {
    let mut moments = moments_spec();
    moments.actions.iter_mut().filter(|action| action.name != "destroy").for_each(|action| action.on_failure = OnFailure::Keep);
    vec![users_spec(), moments, vibes_spec()]
}

pub struct Rig {
    pub sync: Sync,
    pub db: Db,
    pub transport: MockTransport,
    pub clock: ManualClock,
}

pub fn rig() -> Rig {
    rig_with(SyncConfig::default())
}

pub fn register_operations(sync: &Sync) {
    for (name, root) in [
        ("MomentsFeed", "feed"),
        ("MomentsHistory", "history"),
        ("MomentsByUser", "moments"),
        ("MomentsPulse", "feed"),
        ("CurrentUser", "me"),
        ("MomentEvents", "momentEvent"),
        ("CreateMoment", "createMoment"),
        ("UpdateMoment", "updateMoment"),
        ("DeleteMoment", "deleteMoment"),
        ("LikeMoment", "likeMoment"),
    ] {
        sync.register(OperationDef::new(name, &format!("operation {name} {{ x }}"), root).paged(2));
    }
}

pub fn keeping_rig() -> Rig {
    rig_on(Db::open_in_memory(keeping_specs(), None).unwrap(), MockTransport::new(), SyncConfig::default())
}

pub fn rig_with(config: SyncConfig) -> Rig {
    let db = Db::open_in_memory(specs(), None).unwrap();
    rig_on(db, MockTransport::new(), config)
}

pub fn rig_on(db: Db, transport: MockTransport, config: SyncConfig) -> Rig {
    let clock = ManualClock::new(Duration::from_millis(START_MS));
    let sync = Sync::new(db.clone(), Arc::new(transport.clone()), Arc::new(clock.clone()), config);
    register_operations(&sync);
    Rig { sync, db, transport, clock }
}

pub fn params(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

pub fn moment(id: &str, text: &str) -> Value {
    json!({"id": id, "text": text, "userId": null})
}

pub fn feed_page(nodes: Vec<Value>, has_next: bool, cursor: &str) -> Value {
    json!({"feed": {"nodes": nodes, "pageInfo": {"hasNextPage": has_next, "endCursor": cursor}}})
}

pub async fn settle() {
    tokio::time::sleep(Duration::from_millis(1)).await;
}

pub fn member_ids(rig: &Rig, model: &str, relation: &str, params: &Map<String, Value>) -> Vec<String> {
    let spec = rig.db.spec(model).unwrap();
    let relation = spec.relations.iter().find(|candidate| candidate.name == relation).unwrap();
    let strings = params.iter().map(|(name, value)| (name.clone(), value.as_str().unwrap().to_string())).collect();
    let key = relation.scope_key(model, &strings).unwrap();
    rig.db.shared.store.scope_members(model, key.as_str()).unwrap().into_iter().map(|(id, _)| id).collect()
}

pub fn text_of(rig: &Rig, model: &str, id: &str) -> Option<String> {
    let row = rig.db.find(model, &crate::identity::RowId::new(id).unwrap()).unwrap()?;
    row.text("text").map(str::to_string)
}

pub fn feed_reply(ids: &[&str], has_next: bool, cursor: &str) -> crate::transport::Reply {
    crate::transport::Reply::data(feed_page(ids.iter().map(|id| moment(id, id)).collect(), has_next, cursor))
}

pub fn put(rig: &Rig, model: &str, row: Value) {
    use crate::commit::{CommitEnvelope, EntityOp, WriteOrigin};
    let row = row.as_object().unwrap().clone();
    let id = crate::identity::RowId::new(row["id"].as_str().unwrap()).unwrap();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, rig.db.generation());
    envelope.entity_ops.push(EntityOp::Upsert { model: crate::identity::ModelKey::new(model), id, row });
    rig.db.land(envelope).unwrap();
}
