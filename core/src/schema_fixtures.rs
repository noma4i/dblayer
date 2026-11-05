//! Model declarations the tests share: users, moments with a feed, vibes.

use std::collections::BTreeMap;

use serde_json::{json, Map};

use crate::schema::{
    ActionDeclaration, ActionLanding, AssociationKind, AssociationSpec, Coverage, FieldSpec, ModelSpec, PageDirection, RelationSpec,
    RemoteShape, RemoteSpec, RootShape, UpdatesSpec, EphemeralSpec, MergeRule,
};
use crate::value::ScalarKind;

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
    let mut foreground_feed = RelationSpec::global("foreground_feed");
    foreground_feed.remote = Some(remote("MomentsFeed", RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: Some(2), cursor: None }, "5.minutes"));
    foreground_feed.edge_fields = edge_fields();
    foreground_feed.refresh_on_foreground = true;
    let mut vibe_feed = RelationSpec::new("vibe_feed", &[]);
    vibe_feed.text_params = vec!["vibeId".to_string()];
    vibe_feed.remote = Some(remote("MomentsFeed", RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: Some(2), cursor: None }, "5.minutes"));
    vibe_feed.edge_fields = edge_fields();
    vibe_feed.updates = vec![UpdatesSpec { apply: None, subscription: "VibeEvents".to_string(), deleted_by: None, deleted_unless: None, set: Map::new(), removes: false, debounce: None, root: None, id: Vec::new() }];
    let mut since = RelationSpec::new("since", &[]);
    since.text_params = vec!["afterSequence".to_string(), "pinned".to_string()];
    since.scalar_types = BTreeMap::from([("afterSequence".to_string(), ScalarKind::Int), ("pinned".to_string(), ScalarKind::Bool)]);
    since.remote = Some(remote("MomentsFeed", RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: Some(2), cursor: None }, "5.minutes"));
    let mut tail = RelationSpec::new("tail", &[]);
    tail.text_params = vec!["afterSequence".to_string(), "pinned".to_string()];
    tail.scalar_types = since.scalar_types.clone();
    tail.remote = Some(remote("MomentsFeed", RemoteShape::Connection { direction: PageDirection::Backward, coverage: Coverage::Page, max_pages: None, cursor: None }, "5.minutes"));
    let mut user_since = RelationSpec::new("user_since", &["userId"]);
    user_since.text_params = vec!["afterSequence".to_string()];
    user_since.scalar_types = BTreeMap::from([("afterSequence".to_string(), ScalarKind::Int)]);
    user_since.remote = Some(remote("MomentsByUser", RemoteShape::List { coverage: Coverage::Complete }, "1.minute"));
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
        UpdatesSpec { apply: None, subscription: "MomentEvents".to_string(), deleted_by: Some("destroyed".to_string()), deleted_unless: None, set: Map::new(), removes: false, debounce: None, root: None, id: Vec::new() },
        UpdatesSpec { apply: None, subscription: "MomentRemoved".to_string(), deleted_by: None, deleted_unless: None, set: Map::new(), removes: true, debounce: None, root: None, id: Vec::new() },
    ];
    pulse.remote = Some(pulse_remote);
    spec.relations.extend([feed, foreground_feed, vibe_feed, since, tail, user_since, history, by_user, by_mood, search, empty_aware, pulse, RelationSpec::global("recent")]);
    spec.listens = vec![UpdatesSpec { apply: None, subscription: "MomentListen".to_string(), deleted_by: None, deleted_unless: None, set: Map::new(), removes: false, debounce: None, root: None, id: Vec::new() },
        UpdatesSpec { apply: None, subscription: "MomentWrapped".to_string(), deleted_by: None, deleted_unless: None, set: Map::new(), removes: false, debounce: None, root: Some("momentWrapped.wrapped".to_string()), id: vec!["key".to_string()] },
        UpdatesSpec { apply: None, subscription: "MomentDebounced".to_string(), deleted_by: None, deleted_unless: None, set: Map::new(), removes: false, debounce: Some("300.milliseconds".to_string()), root: None, id: Vec::new() },
    ];
    spec.actions = vec![
        bound("create", RootShape::Upsert, "CreateMoment"),
        bound("update", RootShape::Upsert, "UpdateMoment"),
        bound("destroy", RootShape::Destroy, "DeleteMoment"),
        like_action(),
        wrapped_action(),
        ignoring_action(),
        upload_declared("upload"),
        upload_declared("cover"),
    ];
    spec
}

pub fn upload_declared(name: &str) -> ActionDeclaration {
    let mut action = ActionDeclaration::new(name, RootShape::Upsert);
    action.upload = true;
    action.mutation = Some("CreateMoment".to_string());
    action
}

fn edge_fields() -> Vec<FieldSpec> {
    vec![FieldSpec::scalar("impressionUuid", ScalarKind::Str).nullable(), FieldSpec::scalar("impressionScore", ScalarKind::Int).nullable()]
}

fn bound(name: &str, shape: RootShape, mutation: &str) -> ActionDeclaration {
    let mut action = ActionDeclaration::new(name, shape);
    action.mutation = Some(mutation.to_string());
    action.durable = true;
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

use crate::schema::{Sort, SortField};
use crate::value::SortDirection;

pub const CHAT: &str = "c1";

pub fn messages_spec(stable: bool) -> ModelSpec {
    let mut spec = ModelSpec::new(
        "messages",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("chatId", ScalarKind::Id).indexed(),
            FieldSpec::scalar("text", ScalarKind::Str).optional(),
            FieldSpec::scalar("sequenceNumber", ScalarKind::Int).nullable().optional(),
            FieldSpec::scalar("createdAt", ScalarKind::Date).optional(),
        ],
    );
    let mut thread = RelationSpec::new("thread", &["chatId"]);
    thread.remote = Some(remote("MessagesThread", RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None }, "1.minute"));
    thread.joins_events = true;
    thread.order_stable = stable;
    thread.sort = Sort::Fields(vec![
        SortField { column: "sequenceNumber".to_string(), direction: SortDirection::AscNullsLast },
        SortField { column: "createdAt".to_string(), direction: SortDirection::Asc },
    ]);
    spec.relations.push(thread);
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.mutation = Some("CreateMessage".to_string());
    create.has_optimistic = true;
    create.has_temp_row_retention = true;
    create.variables.insert("chatId".to_string(), "chatId".to_string());
    create.variables.insert("text".to_string(), "text".to_string());
    spec.actions.push(create);
    spec
}

