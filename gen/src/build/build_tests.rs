use dblayer::schema::{Newer, CursorSpec, VariableSource, 
    AssociationKind, Coverage, Dependent, FieldKind, ModelSpec, OnFailure, PageDirection, RemoteShape, RootShape, Sort,
    Storage, UpdatesSpec,
};
use dblayer::value::{ScalarKind, SortDirection};
use serde_json::json;

use crate::parse_models;

fn build_one(body: &str) -> ModelSpec {
    let source = format!("model Sample do\n  schema do\n    id :ownerId, index: true\n    date :createdAt, index: true\n  end\n{body}\nend\n");
    let mut models = parse_models(&source).unwrap();
    assert_eq!(models.len(), 1);
    models.remove(0)
}

fn fields_of(lines: &str) -> Vec<dblayer::schema::FieldSpec> {
    let mut fields = parse_models(&format!("model Sample do\n  schema do\n{lines}\n  end\nend\n")).unwrap().remove(0).fields;
    fields.retain(|field| field.name != "id");
    fields
}

#[test]
fn scalar_words_map_to_scalar_kinds() {
    let fields = fields_of("string :a\nnum :b\nint :c\nbool :d\ndate :e\nid :f");
    let kinds: Vec<ScalarKind> = fields
        .iter()
        .map(|field| match field.kind {
            FieldKind::Scalar { kind } => kind,
            _ => panic!("not scalar"),
        })
        .collect();
    assert_eq!(kinds, vec![ScalarKind::Str, ScalarKind::Num, ScalarKind::Int, ScalarKind::Bool, ScalarKind::Date, ScalarKind::Id]);
}

#[test]
fn structured_words_map_to_kinds() {
    let fields = fields_of("object :o\narray :a\nraw :r\nfile :f\nenum :k, values: [:x, \"y\"]\nlist :t, of: :int");
    assert_eq!(fields[0].kind, FieldKind::Object);
    assert_eq!(fields[1].kind, FieldKind::Array);
    assert_eq!(fields[2].kind, FieldKind::Raw);
    assert_eq!(fields[3].kind, FieldKind::File);
    assert_eq!(fields[4].kind, FieldKind::Enum { values: vec!["x".into(), "y".into()] });
    assert_eq!(fields[5].kind, FieldKind::List { element: ScalarKind::Int });
}

#[test]
fn field_keys_set_flags() {
    let fields = fields_of("string :a, null: true, optional: true, index: true, default: \"x\"\nid :b, id_from: :parent\nfile :c, null: true");
    assert!(fields[0].nullable && fields[0].optional && fields[0].indexed);
    assert_eq!(fields[0].default, Some(json!("x")));
    assert_eq!(fields[1].id_from_parent.as_deref(), Some("parent"));
    assert!(fields[2].nullable && !fields[2].indexed);
}

#[test]
fn defaults_convert_to_json() {
    let fields = fields_of("object :a, default: {}\nint :b, default: 3\nnum :c, default: 1.5\nbool :d, default: false\nstring :e, default: nil\narray :f, default: [1, :two]");
    let defaults: Vec<_> = fields.iter().map(|field| field.default.clone()).collect();
    assert_eq!(defaults, vec![Some(json!({})), Some(json!(3)), Some(json!(1.5)), Some(json!(false)), Some(json!(null)), Some(json!([1, "two"]))]);
}

#[test]
fn associations_and_composite_index() {
    let model = parse_models(
        "model Owner do\n  schema do\n    string :n\n  end\nend\nmodel Sample do\n  schema do\n    id :ownerId, index: true\n    date :createdAt, index: true\n    list :peers, of: :id\n    index :ownerId, :createdAt\n  end\n  belongs_to :owner, model: Owner, key: :ownerId, touch: true\n  has_one :first, model: Sample, key: :ownerId, dependent: :destroy\n  references :peers, model: Owner\nend\n",
    )
    .unwrap()
    .remove(1);
    assert_eq!(model.composite_indexes, vec![vec!["ownerId".to_string(), "createdAt".to_string()]]);
    let owner = &model.associations[0];
    assert_eq!((owner.kind, owner.target.as_str(), owner.foreign_key.as_str(), owner.touches_parent), (AssociationKind::BelongsTo, "Owner", "ownerId", true));
    assert_eq!(model.associations[1].dependent, Some(Dependent::Destroy));
    assert_eq!(model.associations[2].kind, AssociationKind::References);
    assert_eq!(model.associations[2].foreign_key, "peers");
}

#[test]
fn a_belongs_to_that_sets_parent_fields_keeps_them_and_the_newer_guard() {
    let model = parse_models(
        "model Owner do\n  schema do\n    id :lastId, null: true\n    date :lastAt, null: true\n  end\nend\nmodel Sample do\n  schema do\n    id :ownerId, index: true\n    date :createdAt\n  end\n  belongs_to :owner, model: Owner, key: :ownerId, sets: { lastId: :id, lastAt: :createdAt }, newer: { createdAt: :lastAt }\nend\n",
    )
    .unwrap()
    .remove(1);
    let owner = &model.associations[0];
    assert_eq!(owner.sets.iter().map(|(parent, child)| (parent.as_str(), child.as_str())).collect::<Vec<_>>(), [("lastAt", "createdAt"), ("lastId", "id")]);
    assert_eq!(owner.newer, Some(Newer { child: "createdAt".to_string(), parent: "lastAt".to_string() }));
}

#[test]
fn references_to_the_same_model_keep_their_list_key_unindexed() {
    let models = parse_models("model Node do\n  schema do\n    list :nearIds, of: :id\n  end\n  references :near, model: Node, key: :nearIds\nend\n").unwrap();
    assert!(!models[0].fields.iter().find(|field| field.name == "nearIds").unwrap().indexed);
}

#[test]
fn scope_without_where_is_global_with_sort() {
    let model = build_one("  scope :recent, order: { createdAt: :desc }");
    let relation = &model.relations[0];
    assert!(relation.is_global && relation.remote.is_none() && relation.by_columns.is_empty());
    let Sort::Fields(fields) = &relation.sort else { panic!("sort") };
    assert_eq!((fields[0].column.as_str(), fields[0].direction), ("createdAt", SortDirection::Desc));
}

#[test]
fn scope_with_where_groups_by_columns() {
    let model = build_one("  scope :by_owner, params: [:ownerId], where: { ownerId: :ownerId }, order: { createdAt: :asc }");
    let relation = &model.relations[0];
    assert!(!relation.is_global);
    assert_eq!(relation.by_columns, vec!["ownerId".to_string()]);
}

#[test]
fn plain_scope_has_server_order() {
    let model = build_one("  scope :all_of_them");
    assert_eq!(model.relations[0].sort, Sort::ServerOrder);
}

#[test]
fn remote_with_only_query_is_single_with_default_freshness() {
    let model = build_one("  remote :profile do\n    query Profile\n  end");
    let relation = &model.relations[0];
    assert!(relation.is_global);
    let remote = relation.remote.as_ref().unwrap();
    assert_eq!(remote.shape, RemoteShape::Single);
    assert_eq!(remote.operation, "Profile");
    assert_eq!(remote.stale_time, "5.minutes");
    assert_eq!(remote.empty_stale_time, None);
    assert!(!remote.selects_page_info);
    assert_eq!(remote.coverage(), Coverage::Complete);
}

#[test]
fn remote_list_is_complete_list() {
    let model = build_one("  remote :all do\n    query All\n    list\n    fresh_for 2.hours\n  end");
    let remote = model.relations[0].remote.as_ref().unwrap();
    assert_eq!(remote.shape, RemoteShape::List { coverage: Coverage::Complete });
    assert_eq!(remote.stale_time, "2.hours");
}

#[test]
fn remote_paginate_is_forward_page_connection() {
    let model = build_one("  remote :feed do\n    query Feed\n    paginate 20\n    fresh_for 5.minutes, empty: 30.seconds\n    max_pages 5\n  end");
    let remote = model.relations[0].remote.as_ref().unwrap();
    assert_eq!(remote.shape, RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: Some(5), cursor: None });
    assert!(remote.selects_page_info);
    assert_eq!(remote.empty_stale_time.as_deref(), Some("30.seconds"));
}

#[test]
fn remote_paginate_backward_without_cap() {
    let model = build_one("  remote :feed do\n    query Feed\n    paginate 20, backward: true\n  end");
    let remote = model.relations[0].remote.as_ref().unwrap();
    assert_eq!(remote.shape, RemoteShape::Connection { direction: PageDirection::Backward, coverage: Coverage::Page, max_pages: None, cursor: None });
}

#[test]
fn scopes_and_remotes_keep_written_order() {
    let model = build_one("  remote :b do\n    query B\n  end\n  scope :a\n  remote :c do\n    query C\n  end");
    let names: Vec<_> = model.relations.iter().map(|relation| relation.name.as_str()).collect();
    assert_eq!(names, vec!["b", "a", "c"]);
}

#[test]
fn standard_actions_bind_mutations_and_input() {
    let model = build_one("  create with: CreateSample, input: { text: :createdAt, ownerId: :ownerId }\n  update with: UpdateSample\n  destroy with: DeleteSample");
    let names: Vec<_> = model.actions.iter().map(|action| action.name.as_str()).collect();
    assert_eq!(names, vec!["create", "update", "destroy"]);
    let create = &model.actions[0];
    assert_eq!((create.mutation.as_deref(), create.root_shape), (Some("CreateSample"), RootShape::Upsert));
    assert!(create.has_optimistic && create.has_temp_row_retention);
    assert_eq!(create.variables.get("text").map(String::as_str), Some("createdAt"));
    assert_eq!(model.actions[1].variables.len(), 0);
    let destroy = &model.actions[2];
    assert_eq!((destroy.mutation.as_deref(), destroy.root_shape), (Some("DeleteSample"), RootShape::Destroy));
}

#[test]
fn action_block_collects_mutation_input_optimistic_and_refetch() {
    let model = build_one("  remote :feed do\n    query Feed\n  end\n  action :like do\n    mutation LikeSample\n    input sampleId: :id\n    optimistic createdAt: \"liked\", ownerId: nil\n    refetch :feed\n  end");
    let action = &model.actions[0];
    assert_eq!(action.name, "like");
    assert_eq!((action.root_shape, action.has_optimistic), (RootShape::Upsert, false));
    assert_eq!(action.mutation.as_deref(), Some("LikeSample"));
    assert_eq!(action.variables.get("sampleId").map(String::as_str), Some("id"));
    assert_eq!(action.optimistic.get("createdAt"), Some(&json!("liked")));
    assert_eq!(action.optimistic.get("ownerId"), Some(&json!(null)));
    assert_eq!(action.refetch, vec!["feed".to_string()]);
}

#[test]
fn remote_updates_line_builds_updates_spec() {
    let model = build_one("  remote :feed do\n    query Feed\n    updates SampleEvents, deleted_by: :destroyed\n  end\n  remote :plain do\n    query Plain\n    updates PlainEvents\n  end");
    let updates = |index: usize| model.relations[index].remote.as_ref().unwrap().updates.clone();
    assert_eq!(updates(0), vec![UpdatesSpec { subscription: "SampleEvents".into(), deleted_by: Some("destroyed".into()), set: Default::default(), removes: false, debounce: None, root: None, id: None }]);
    assert_eq!(updates(1), vec![UpdatesSpec { subscription: "PlainEvents".into(), deleted_by: None, set: Default::default(), removes: false, debounce: None, root: None, id: None }]);
}

#[test]
fn on_failure_defaults_to_rollback_and_reads_keep_from_standard_and_block_actions() {
    let model = build_one(
        "  create with: CreateSample, on_failure: :keep\n  update with: UpdateSample\n  destroy with: DeleteSample\n  action :like do\n    mutation LikeSample\n    on_failure :keep\n  end\n  action :poke do\n    mutation PokeSample\n  end",
    );
    let policies: Vec<(&str, OnFailure)> = model.actions.iter().map(|action| (action.name.as_str(), action.on_failure)).collect();
    assert_eq!(
        policies,
        [("create", OnFailure::Keep), ("update", OnFailure::Rollback), ("destroy", OnFailure::Rollback), ("like", OnFailure::Keep), ("poke", OnFailure::Rollback)]
    );
}

#[test]
fn ephemeral_block_builds_ephemeral_specs_beside_stored_fields() {
    let model = parse_models(
        "model Sample do\n  schema do\n    string :name\n    ephemeral do\n      bool :typing, default: false, reset_after: 5.seconds\n      string :mood, null: true\n    end\n  end\nend\n",
    )
    .unwrap()
    .remove(0);
    assert_eq!(model.fields.iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), vec!["id", "name"]);
    let typing = model.ephemeral_field("typing").unwrap();
    assert_eq!((typing.field.default.clone(), typing.reset_after.clone()), (Some(json!(false)), Some("5.seconds".to_string())));
    let mood = model.ephemeral_field("mood").unwrap();
    assert_eq!((mood.field.nullable, mood.reset_after.clone()), (true, None));
}

#[test]
fn model_line_options_build_storage_and_expiry() {
    let memory = parse_models("model Presence, storage: :memory, expire_after: 30.seconds do\n  schema do\n    string :room\n  end\nend\n").unwrap().remove(0);
    assert_eq!((memory.storage, memory.expire_after.as_deref()), (Storage::Memory, Some("30.seconds")));
    let plain = parse_models("model Presence do\n  schema do\n    string :room\n  end\nend\n").unwrap().remove(0);
    assert_eq!((plain.storage, plain.expire_after), (Storage::Disk, None));
}

#[test]
fn updates_set_builds_ephemeral_values() {
    let model = parse_models(
        "model Sample do\n  schema do\n    string :name\n    ephemeral do\n      bool :typing, default: false, reset_after: 5.seconds\n    end\n  end\n  remote :one do\n    query One\n    updates Typing, set: { typing: true }\n  end\nend\n",
    )
    .unwrap()
    .remove(0);
    let updates = model.relations[0].remote.as_ref().unwrap().updates[0].clone();
    assert_eq!(updates.set.get("typing"), Some(&json!(true)));
}

#[test]
fn paginate_with_its_own_cursor_names_variable_and_source() {
    let model = build_one("  remote :feed do\n    query Feed\n    paginate 20, cursor: :afterSequence, from: \"lastSequenceNumber\", cursor_type: :int\n  end");
    let remote = model.relations[0].remote.as_ref().unwrap();
    let cursor = CursorSpec { variable: "afterSequence".to_string(), from: "lastSequenceNumber".to_string(), numeric: true };
    assert_eq!(remote.shape, RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: Some(cursor) });
}

#[test]
fn operations_can_be_named_with_a_string_when_the_graphql_name_is_lowercase() {
    let model = build_one("  remote :feed do\n    query \"feed\"\n  end\n  action :like, mutation: \"likeMoment\"");
    assert_eq!(model.relations[0].remote.as_ref().unwrap().operation, "feed");
    assert_eq!(model.actions.iter().find(|action| action.name == "like").unwrap().mutation.as_deref(), Some("likeMoment"));
}

#[test]
fn variables_line_renames_params_and_fixes_constants() {
    let model = build_one("  remote :list do\n    query \"chats\"\n    variables statusFilter: :status, includeGifts: false, mediaKinds: [\"image\", \"video\"]\n  end");
    let variables = &model.relations[0].remote.as_ref().unwrap().variables;
    assert_eq!(variables["statusFilter"], VariableSource::Param { name: "status".to_string() });
    assert_eq!(variables["includeGifts"], VariableSource::Constant { value: json!(false) });
    assert_eq!(variables["mediaKinds"], VariableSource::Constant { value: json!(["image", "video"]) });
}

#[test]
fn a_relation_can_follow_several_subscriptions() {
    let model = build_one("  remote :list do\n    query \"chats\"\n    updates \"chatCreated\"\n    updates \"chatUpdated\", debounce: 300.milliseconds\n    updates \"chatDeleted\", removes: true\n  end");
    let updates = &model.relations[0].remote.as_ref().unwrap().updates;
    assert_eq!(updates.iter().map(|update| update.subscription.as_str()).collect::<Vec<_>>(), ["chatCreated", "chatUpdated", "chatDeleted"]);
    assert_eq!(updates[1].debounce.as_deref(), Some("300.milliseconds"));
    assert!(updates[2].removes && !updates[0].removes);
}

#[test]
fn a_remote_relation_can_take_params_that_name_columns() {
    let model = build_one("  remote :by_owner, params: [:ownerId] do\n    query \"owned\"\n    paginate 10\n  end");
    let relation = &model.relations[0];
    assert_eq!(relation.by_columns, vec!["ownerId".to_string()]);
    assert!(!relation.is_global && relation.remote.is_some());
}

#[test]
fn a_relation_can_take_the_id_as_its_param() {
    let model = build_one("  remote :details, params: [:id] do\n    query \"sample\"\n  end");
    assert_eq!(model.relations[0].by_columns, vec!["id".to_string()]);
    assert!(!model.field("id").unwrap().indexed);
    assert!(dblayer::schema::validate_models(&[model], &[]).is_empty());
}

#[test]
fn a_remote_relation_can_order_its_rows_locally() {
    let model = build_one("  remote :recent, order: { createdAt: :desc } do\n    query \"recent\"\n  end");
    assert_eq!(model.relations[0].sort, Sort::Fields(vec![dblayer::schema::SortField { column: "createdAt".to_string(), direction: SortDirection::Desc }]));
}
