use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Map, Value};

use crate::db::Db;
use crate::pacing::ManualClock;
use crate::schema::{
    ActionDeclaration, ActionLanding, AssociationKind, AssociationSpec, Coverage, FieldSpec, ModelSpec, OnFailure, PageDirection, RelationSpec,
    RemoteShape, RemoteSpec, RootShape, UpdatesSpec, EphemeralSpec, MergeRule,
};
use crate::transport::MockTransport;
use crate::value::ScalarKind;

use super::{OperationDef, Sync, SyncConfig};

pub const START_MS: u64 = 1_000_000;

pub fn remote(operation: &str, shape: RemoteShape, stale: &str) -> RemoteSpec {
    let selects_page_info = matches!(shape, RemoteShape::Connection { .. });
    RemoteSpec { operation: operation.to_string(), shape, stale_time: stale.to_string(), empty_stale_time: None, selects_page_info, page_size: None, variables: Default::default(), root: None, total_from: None }
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
            FieldSpec::enumeration("mood", &["calm", "wild"]).optional().indexed(),
            FieldSpec { merge: Some(MergeRule::Local), ..FieldSpec::scalar("tone", ScalarKind::Str).optional() },
            FieldSpec { merge: Some(MergeRule::Keys { keys: BTreeMap::from([("tone".to_string(), MergeRule::Local)]) }), ..FieldSpec::object("cover").optional() },
        ],
    );
    spec.associations.push(AssociationSpec::new("author", AssociationKind::BelongsTo, "users", "userId"));
    let forward = RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: Some(2), cursor: None };
    let backward = RemoteShape::Connection { direction: PageDirection::Backward, coverage: Coverage::Page, max_pages: None, cursor: None };
    let mut feed = RelationSpec::global("feed");
    feed.remote = Some(remote("MomentsFeed", forward, "5.minutes"));
    feed.edge_fields = edge_fields();
    let mut vibe_feed = RelationSpec::new("vibe_feed", &[]);
    vibe_feed.text_params = vec!["vibeId".to_string()];
    vibe_feed.remote = Some(remote("MomentsFeed", RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: Some(2), cursor: None }, "5.minutes"));
    vibe_feed.edge_fields = edge_fields();
    let mut since = RelationSpec::new("since", &[]);
    since.text_params = vec!["afterSequence".to_string(), "pinned".to_string()];
    since.scalar_types = BTreeMap::from([("afterSequence".to_string(), ScalarKind::Int), ("pinned".to_string(), ScalarKind::Bool)]);
    since.remote = Some(remote("MomentsFeed", RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: Some(2), cursor: None }, "5.minutes"));
    let mut tail = RelationSpec::new("tail", &[]);
    tail.text_params = vec!["afterSequence".to_string(), "pinned".to_string()];
    tail.scalar_types = since.scalar_types.clone();
    tail.remote = Some(remote("MomentsFeed", RemoteShape::Connection { direction: PageDirection::Backward, coverage: Coverage::Page, max_pages: None, cursor: None }, "5.minutes"));
    let mut history = RelationSpec::global("history");
    history.remote = Some(remote("MomentsHistory", backward, "5.minutes"));
    let mut by_user = RelationSpec::new("by_user", &["userId"]);
    by_user.remote = Some(remote("MomentsByUser", RemoteShape::List { coverage: Coverage::Complete }, "1.minute"));
    by_user.joins_events = true;
    let mut by_mood = RelationSpec::new("by_mood", &["mood"]);
    by_mood.remote = Some(remote("MomentsByMood", RemoteShape::List { coverage: Coverage::Complete }, "1.minute"));
    let mut search = RelationSpec::new("search", &[]);
    search.text_params = vec!["query".to_string()];
    search.remote = Some(remote("MomentsSearch", RemoteShape::List { coverage: Coverage::Complete }, "1.minute"));
    let mut empty_aware = RelationSpec::global("empty_aware");
    let mut spec_remote = remote("MomentsByUser", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes");
    spec_remote.empty_stale_time = Some("10.seconds".to_string());
    empty_aware.remote = Some(spec_remote);
    let mut pulse = RelationSpec::global("pulse");
    let pulse_remote = remote("MomentsPulse", RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: Some(2), cursor: None }, "5.minutes");
    pulse.updates = vec![
        UpdatesSpec { apply: None, subscription: "MomentEvents".to_string(), deleted_by: Some("destroyed".to_string()), set: Map::new(), removes: false, debounce: None, root: None, id: None },
        UpdatesSpec { apply: None, subscription: "MomentRemoved".to_string(), deleted_by: None, set: Map::new(), removes: true, debounce: None, root: None, id: None },
    ];
    pulse.remote = Some(pulse_remote);
    spec.relations.extend([feed, vibe_feed, since, tail, history, by_user, by_mood, search, empty_aware, pulse, RelationSpec::global("recent")]);
    spec.listens = vec![UpdatesSpec { apply: None, subscription: "MomentListen".to_string(), deleted_by: None, set: Map::new(), removes: false, debounce: None, root: None, id: None },
        UpdatesSpec { apply: None, subscription: "MomentWrapped".to_string(), deleted_by: None, set: Map::new(), removes: false, debounce: None, root: Some("momentWrapped.wrapped".to_string()), id: Some("key".to_string()) },
        UpdatesSpec { apply: None, subscription: "MomentDebounced".to_string(), deleted_by: None, set: Map::new(), removes: false, debounce: Some("300.milliseconds".to_string()), root: None, id: None },
    ];
    spec.actions = vec![
        bound("create", RootShape::Upsert, "CreateMoment"),
        bound("update", RootShape::Upsert, "UpdateMoment"),
        bound("destroy", RootShape::Destroy, "DeleteMoment"),
        like_action(),
        wrapped_action(),
        ignoring_action(),
    ];
    spec
}

fn edge_fields() -> Vec<FieldSpec> {
    vec![FieldSpec::scalar("impressionUuid", ScalarKind::Str).nullable(), FieldSpec::scalar("impressionScore", ScalarKind::Int).nullable()]
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

fn wrapped_action() -> ActionDeclaration {
    let mut action = bound("star", RootShape::Upsert, "StarMoment");
    action.variables.insert("momentId".to_string(), "id".to_string());
    action.wrap = Some("input".to_string());
    action.root = Some("starMoment.moment".to_string());
    action.lands.push(ActionLanding { model: "users".to_string(), root: "starMoment.viewer".to_string() });
    action.result.insert("successMessage".to_string(), "string".to_string());
    action.result.insert("copies".to_string(), "int".to_string());
    action
}

fn ignoring_action() -> ActionDeclaration {
    let mut action = bound("poke", RootShape::Upsert, "PokeMoment");
    action.variables.insert("momentId".to_string(), "id".to_string());
    action.ignore_response = true;
    action
}

fn like_action() -> ActionDeclaration {
    let mut action = bound("like", RootShape::Upsert, "LikeMoment");
    action.variables.insert("momentId".to_string(), "id".to_string());
    action.optimistic.insert("likedByMe".to_string(), json!(true));
    action.refetch.push("history".to_string());
    action.refetch.push("users.current".to_string());
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
        ("MomentsSearch", "moments"),
        ("MomentListen", "momentListen"),
        ("MomentWrapped", "momentWrapped"),
        ("MomentDebounced", "momentDebounced"),
        ("MomentsByMood", "moments"),
        ("MomentsPulse", "feed"),
        ("CurrentUser", "me"),
        ("MomentEvents", "momentEvent"),
        ("MomentRemoved", "momentRemoved"),
        ("CreateMoment", "createMoment"),
        ("UpdateMoment", "updateMoment"),
        ("DeleteMoment", "deleteMoment"),
        ("LikeMoment", "likeMoment"),
        ("StarMoment", "starMoment"),
        ("PokeMoment", "pokeMoment"),
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
    let rig = rig_without_operations(db, transport, config);
    register_operations(&rig.sync);
    rig
}

pub fn rig_without_operations(db: Db, transport: MockTransport, config: SyncConfig) -> Rig {
    let clock = ManualClock::new(Duration::from_millis(START_MS));
    let sync = Sync::new(db.clone(), Arc::new(transport.clone()), Arc::new(clock.clone()), config);
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
    let strings = params.iter().map(|(name, value)| (name.clone(), crate::value::relation_param_text(value))).collect();
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
