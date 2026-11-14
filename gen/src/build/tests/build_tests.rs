use dblayer::schema::{
    AssociationKind, Coverage, CursorSpec, Dependent, FieldKind, MergeRule, ModelSpec, OnFailure, PageDirection, RemoteShape, RootShape, Sort, Storage, UpdatesSpec, VariableSource,
};
use dblayer::value::{ScalarKind, SortDirection};
use serde_json::json;

use crate::check::check_source;

fn build_one(body: &str) -> ModelSpec {
    let source = format!("model Sample do\n  attribute :ownerId, :id\n  attribute :createdAt, :date\n  index :ownerId, :createdAt\n{body}\nend\n");
    let mut models = check_source(&source).unwrap();
    assert_eq!(models.len(), 1);
    models.remove(0)
}

fn fields_of(lines: &str) -> Vec<dblayer::schema::FieldSpec> {
    let mut fields = check_source(&format!("model Sample do\n{lines}\nend\n")).unwrap().remove(0).fields;
    fields.retain(|field| field.name != "id");
    fields
}

#[test]
fn type_words_map_to_field_kinds() {
    let fields = fields_of("attribute :a, :string\nattribute :b, :num\nattribute :c, :int\nattribute :d, :bool\nattribute :e, :date\nattribute :f, :id\nattribute :r, :json\nattribute :file, :file\nattribute :k, :enum, values: [\"x\", \"y\"]\nattribute :t, [:int]");
    let kinds: Vec<FieldKind> = fields.iter().map(|field| field.kind.clone()).collect();
    assert_eq!(
        kinds,
        vec![
            FieldKind::Scalar { kind: ScalarKind::Str },
            FieldKind::Scalar { kind: ScalarKind::Num },
            FieldKind::Scalar { kind: ScalarKind::Int },
            FieldKind::Scalar { kind: ScalarKind::Bool },
            FieldKind::Scalar { kind: ScalarKind::Date },
            FieldKind::Scalar { kind: ScalarKind::Id },
            FieldKind::Raw,
            FieldKind::File,
            FieldKind::Enum { values: vec!["x".into(), "y".into()] },
            FieldKind::List { element: ScalarKind::Int },
        ]
    );
}

#[test]
fn a_named_type_with_a_block_is_an_object_and_a_list_of_it_is_an_array() {
    let fields = fields_of("attribute :o, Inner do\n  attribute :x, :int\nend\nattribute :a, [Cell] do\n  attribute :y, :int\nend");
    assert_eq!((fields[0].kind.clone(), fields[0].app_type.as_deref()), (FieldKind::Object, Some("Inner")));
    assert_eq!((fields[1].kind.clone(), fields[1].app_type.as_deref()), (FieldKind::Array, Some("Cell")));
}

#[test]
fn attribute_options_set_flags() {
    let fields = fields_of("attribute :a, :string, null: true, optional: true, default: \"x\"\nattribute :b, :id, id_from: :parent\nattribute :c, :string, null: true, from: \"c.label\"");
    assert!(fields[0].nullable && fields[0].optional && !fields[0].indexed);
    assert_eq!(fields[0].default, Some(json!("x")));
    assert_eq!(fields[1].id_from_parent.as_deref(), Some("parent"));
    assert_eq!(fields[2].from.as_deref(), Some("c.label"));
}

#[test]
fn a_nested_link_reaches_the_field_spec() {
    let fields = fields_of("attribute :a, [Cell] do\n  attribute :ownerId, :id\n  attribute :o, Inner, null: true, ref: Sample, id: :ownerId do\n    attribute :x, :int, null: true\n  end\nend");
    let link = fields[0].fields[1].link.clone().unwrap();
    assert_eq!((link.model.as_str(), link.id.as_str()), ("Sample", "ownerId"));
}

#[test]
fn defaults_convert_to_json() {
    let fields = fields_of("attribute :b, :int, default: 3\nattribute :c, :num, default: 1.5\nattribute :d, :bool, default: false\nattribute :e, :string, null: true, default: nil\nattribute :f, [:string], default: [\"one\", :two]");
    let defaults: Vec<_> = fields.iter().map(|field| field.default.clone()).collect();
    assert_eq!(defaults, vec![Some(json!(3)), Some(json!(1.5)), Some(json!(false)), Some(json!(null)), Some(json!(["one", "two"]))]);
}

#[test]
fn a_search_line_marks_the_attribute_and_an_index_line_keeps_its_columns() {
    let model = build_one("  attribute :title, :string\n  search :title");
    assert!(model.field("title").unwrap().searchable);
    assert_eq!(model.indexes, vec![vec!["ownerId".to_string(), "createdAt".to_string()]]);
}

#[test]
fn associations_and_composite_index() {
    let model = check_source(
        "model Owner do\nend\n\nmodel Sample do\n  attribute :ownerId, :id\n  attribute :createdAt, :date\n  attribute :peers, [:id], default: []\n  index :ownerId, :createdAt\n  belongs_to :owner, class_name: Owner, foreign_key: :ownerId, touch: true\n  has_one :latest, class_name: Sample, foreign_key: :ownerId, dependent: :destroy\n  references :peers, class_name: Owner\nend\n",
    )
    .unwrap()
    .remove(1);
    assert_eq!(model.indexes, vec![vec!["ownerId".to_string(), "createdAt".to_string()]]);
    let owner = &model.associations[0];
    assert_eq!((owner.kind, owner.target.as_str(), owner.foreign_key.as_str(), owner.touches_parent), (AssociationKind::BelongsTo, "Owner", "ownerId", true));
    assert_eq!(model.associations[1].dependent, Some(Dependent::Destroy));
    assert_eq!(model.associations[2].kind, AssociationKind::References);
    assert_eq!(model.associations[2].foreign_key, "peers");
}

#[test]
fn a_belongs_to_that_sets_derived_parent_fields_keeps_them_and_the_newer_list() {
    let model = check_source(
        "model Owner do\n  attribute :lastId, :id, derived: true\n  attribute :lastAt, :date, derived: true\n  attribute :lastSeq, :int, derived: true\nend\n\nmodel Sample do\n  attribute :ownerId, :id\n  attribute :createdAt, :date\n  attribute :seq, :int\n  index :ownerId\n  belongs_to :owner, class_name: Owner, foreign_key: :ownerId, sets: { lastId: :id, lastAt: :createdAt, lastSeq: :seq }, newer: [:seq, :createdAt]\nend\n",
    )
    .unwrap()
    .remove(1);
    let owner = &model.associations[0];
    assert_eq!(owner.sets.iter().map(|(parent, child)| (parent.as_str(), child.as_str())).collect::<Vec<_>>(), [("lastAt", "createdAt"), ("lastId", "id"), ("lastSeq", "seq")]);
    assert_eq!(owner.newer, ["seq", "createdAt"]);
}

#[test]
fn references_to_the_same_model_keep_their_list_key_unindexed() {
    let models = check_source("model Node do\n  references :near, class_name: Node, foreign_key: :nearIds\n  attribute :nearIds, [:id], default: []\nend\n").unwrap();
    assert!(!models[0].fields.iter().find(|field| field.name == "nearIds").unwrap().indexed);
}

#[test]
fn a_local_scope_without_where_is_global_with_its_sort() {
    let model = build_one("  scope :recent do\n    order createdAt: :desc\n  end");
    let relation = &model.relations[0];
    assert!(relation.is_global && relation.remote.is_none() && relation.by_columns.is_empty());
    let Sort::Fields(fields) = &relation.sort else { panic!("sort") };
    assert_eq!((fields[0].column.as_str(), fields[0].direction), ("createdAt", SortDirection::Desc));
}

#[test]
fn a_local_scope_with_where_groups_by_columns() {
    let model = build_one("  scope :by_owner do\n    parameter :ownerId, :id\n    where ownerId: :ownerId\n    order createdAt: :asc\n  end");
    let relation = &model.relations[0];
    assert!(!relation.is_global);
    assert_eq!(relation.by_columns, vec!["ownerId".to_string()]);
}

#[test]
fn a_scope_without_order_keeps_the_server_order() {
    let model = build_one("  scope :all_of_them do\n  end");
    assert_eq!(model.relations[0].sort, Sort::ServerOrder);
}

#[test]
fn a_query_alone_is_a_single_read_with_default_freshness() {
    let model = build_one("  scope :profile do\n    query \"Profile\"\n  end");
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
fn a_list_response_is_a_complete_list() {
    let model = build_one("  scope :all do\n    query \"All\"\n    response shape: :list\n    freshness expires_in: 2.hours\n  end");
    let remote = model.relations[0].remote.as_ref().unwrap();
    assert_eq!(remote.shape, RemoteShape::List { coverage: Coverage::Complete });
    assert_eq!(remote.stale_time, "2.hours");
}

#[test]
fn paginate_is_a_forward_page_connection_with_its_cap_and_freshness() {
    let model = build_one("  scope :feed do\n    query \"Feed\"\n    paginate size: 20, direction: :forward, max_pages: 5\n    freshness expires_in: 5.minutes, empty_in: 30.seconds\n  end");
    let remote = model.relations[0].remote.as_ref().unwrap();
    assert_eq!(remote.shape, RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: Some(5), cursor: None });
    assert_eq!(remote.page_size, Some(20));
    assert!(remote.selects_page_info);
    assert_eq!(remote.empty_stale_time.as_deref(), Some("30.seconds"));
}

#[test]
fn paginate_backward_without_a_cap() {
    let model = build_one("  scope :feed do\n    query \"Feed\"\n    paginate size: 20, direction: :backward\n  end");
    let remote = model.relations[0].remote.as_ref().unwrap();
    assert_eq!(remote.shape, RemoteShape::Connection { direction: PageDirection::Backward, coverage: Coverage::Page, max_pages: None, cursor: None });
}

#[test]
fn paginate_with_its_own_cursor_names_variable_source_and_type() {
    let model = build_one("  scope :feed do\n    query \"Feed\"\n    paginate size: 20, direction: :forward, cursor: { parameter: \"afterSequence\", from: \"lastSequenceNumber\", type: :int }\n  end");
    let remote = model.relations[0].remote.as_ref().unwrap();
    let cursor = CursorSpec { variable: "afterSequence".to_string(), from: "lastSequenceNumber".to_string(), numeric: true };
    assert_eq!(remote.shape, RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: Some(cursor) });
}

#[test]
fn a_string_cursor_is_not_numeric() {
    let model = build_one("  scope :feed do\n    query \"Feed\"\n    paginate size: 20, direction: :forward, cursor: { parameter: \"after\", from: \"last\", type: :string }\n  end");
    let RemoteShape::Connection { cursor: Some(cursor), .. } = &model.relations[0].remote.as_ref().unwrap().shape else { panic!("cursor") };
    assert!(!cursor.numeric);
}

#[test]
fn standard_operations_bind_mutations_and_variables() {
    let model = build_one("  operation :create do\n    mutation \"CreateSample\"\n    variables text: :createdAt, ownerId: :ownerId\n  end\n  operation :update do\n    mutation \"UpdateSample\"\n  end\n  operation :destroy do\n    mutation \"DeleteSample\"\n  end");
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
fn a_named_operation_collects_mutation_variables_optimistic_and_refetch() {
    let model = build_one("  scope :feed do\n    query \"Feed\"\n  end\n  operation :like do\n    mutation \"LikeSample\"\n    variables sampleId: :id\n    optimistic ownerId: nil, createdAt: \"liked\"\n    refetch :feed\n  end");
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
fn refetch_names_a_scope_of_another_model_with_its_model() {
    let models = check_source("model Other do\n  scope :list do\n    query \"Others\"\n    response shape: :list\n  end\nend\n\nmodel Sample do\n  operation :poke do\n    mutation \"Poke\"\n    optimistic false\n    refetch :list, model: Other\n    refetch :list, model: Sample\n  end\n  scope :list do\n    query \"Samples\"\n    response shape: :list\n  end\nend\n").unwrap();
    assert_eq!(models[1].actions[0].refetch, vec!["Other.list".to_string(), "list".to_string()]);
}

#[test]
fn insert_on_server_joins_events() {
    let model = build_one("  scope :mine, insert_on: :server do\n    parameter :ownerId, :id\n    where ownerId: :ownerId\n    query \"Mine\"\n  end\n  scope :other, insert_on: :create do\n    query \"Other\"\n  end");
    let flags: Vec<(bool, bool)> = model.relations.iter().map(|relation| (relation.joins_events, relation.joins_created)).collect();
    assert_eq!(flags, [(true, false), (false, true)]);
}

#[test]
fn insert_on_server_joins_node_only_with_the_node_read_and_a_node_change() {
    let node = "  scope :node, node: :one do\n    parameter :id, :id\n    query \"SampleNode\"\n    variables id: :id\n  end\n";
    let list = "  scope :mine, insert_on: :server do\n    parameter :ownerId, :id\n    where ownerId: :ownerId\n    query \"Mine\"\n  end\n";
    let changed = "  changes \"SampleEvents\", from: \"sampleEvents.entries\", area: \"topic\", kind: \"action\", id: \"recordId\"\n  changed_by \"SAMPLE\", via: :node\n";
    let flags = |body: String| {
        let model = build_one(&body);
        let relation = model.relations.iter().find(|relation| relation.name == "mine").unwrap();
        (relation.joins_events, relation.joins_node)
    };
    assert_eq!(flags(format!("{node}{list}{changed}")), (true, true));
    assert_eq!(flags(format!("{node}{list}")), (true, false));
    assert_eq!(flags(format!("{list}")), (true, false));
}

#[test]
fn insert_on_create_marks_only_the_relation_that_names_it() {
    let model = build_one("  scope :mine, insert_on: :create do\n    query \"Mine\"\n  end\n  scope :other do\n    query \"Other\"\n  end");
    let flags: Vec<(bool, bool)> = model.relations.iter().map(|relation| (relation.joins_created, relation.joins_events)).collect();
    assert_eq!(flags, [(true, false), (false, false)]);
}

#[test]
fn mapped_optimistic_builds_an_op_that_names_its_parameter_and_cases() {
    let model = build_one("  operation :decide do\n    mutation \"DecideSample\"\n    parameter :decision, Decision\n    optimistic ownerId: { from: :decision, map: { ACCEPT: \"yes\", REJECT: \"no\" } }\n  end");
    assert_eq!(model.actions[0].optimistic.get("ownerId"), Some(&json!({"$map": {"from": "decision", "map": {"ACCEPT": "yes", "REJECT": "no"}}})));
}

#[test]
fn keyed_optimistic_builds_an_op_that_names_its_parameters() {
    let model = build_one("  operation :tag do\n    mutation \"TagSample\"\n    parameter :label, :string\n    optimistic ownerId: { by: :ownerId, element: { ownerId: :label, createdAt: nil } }\n  end");
    assert_eq!(model.actions[0].optimistic.get("ownerId"), Some(&json!({"$keyed": {"by": "ownerId", "element": {"ownerId": {"$arg": "label"}, "createdAt": null}}})));
}

#[test]
fn a_parameter_symbol_in_optimistic_becomes_an_arg_reference() {
    let model = build_one("  operation :mute do\n    mutation \"Mute\"\n    parameter :mute, :bool\n    optimistic createdAt: :mute\n  end");
    assert_eq!(model.actions[0].optimistic.get("createdAt"), Some(&json!({"$arg": "mute"})));
    assert_eq!(model.actions[0].args.get("mute").map(String::as_str), Some("bool"));
}

#[test]
fn subscribe_in_a_scope_builds_updates_specs() {
    let model = build_one("  scope :feed do\n    query \"Feed\"\n    subscribe \"SampleEvents\", apply: { effect: :upsert, delete_if: \"destroyed\" }\n    subscribe \"PlainEvents\"\n  end");
    let updates = &model.relations[0].updates;
    let apply = Some(dblayer::schema::SubscriptionApply { action: dblayer::schema::SubscriptionAction::Upsert, matches: Default::default() });
    assert_eq!(updates[0], UpdatesSpec { apply: apply.clone(), subscription: "SampleEvents".into(), deleted_by: Some("destroyed".into()), deleted_unless: None, set: Default::default(), debounce: None, root: None, id: Vec::new() });
    assert_eq!(updates[1], UpdatesSpec { apply, subscription: "PlainEvents".into(), deleted_by: None, deleted_unless: None, set: Default::default(), debounce: None, root: None, id: Vec::new() });
}

#[test]
fn on_failure_defaults_to_rollback_and_reads_keep_from_every_operation() {
    let model = build_one(
        "  operation :create do\n    mutation \"CreateSample\"\n    on_failure :keep\n  end\n  operation :update do\n    mutation \"UpdateSample\"\n  end\n  operation :destroy do\n    mutation \"DeleteSample\"\n  end\n  operation :like do\n    mutation \"LikeSample\"\n    optimistic false\n    on_failure :keep\n  end\n  operation :poke do\n    mutation \"PokeSample\"\n    optimistic false\n  end",
    );
    let policies: Vec<(&str, OnFailure)> = model.actions.iter().map(|action| (action.name.as_str(), action.on_failure)).collect();
    assert_eq!(
        policies,
        [("create", OnFailure::Keep), ("update", OnFailure::Rollback), ("destroy", OnFailure::Rollback), ("like", OnFailure::Keep), ("poke", OnFailure::Rollback)]
    );
}

#[test]
fn ephemeral_block_builds_ephemeral_specs_beside_stored_attributes() {
    let model = check_source("model Sample do\n  attribute :name, :string\n  ephemeral do\n    attribute :typing, :bool, default: false, reset_after: 5.seconds\n    attribute :mood, :string, null: true\n  end\nend\n").unwrap().remove(0);
    assert_eq!(model.fields.iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), vec!["id", "name"]);
    let typing = model.ephemeral_field("typing").unwrap();
    assert_eq!((typing.field.default.clone(), typing.reset_after.clone()), (Some(json!(false)), Some("5.seconds".to_string())));
    let mood = model.ephemeral_field("mood").unwrap();
    assert_eq!((mood.field.nullable, mood.reset_after.clone()), (true, None));
}

#[test]
fn model_line_options_build_storage_and_expiry() {
    let memory = check_source("model Presence, storage: :memory, expires_in: 30.seconds do\n  attribute :room, :string\nend\n").unwrap().remove(0);
    assert_eq!((memory.storage, memory.expire_after.as_deref()), (Storage::Memory, Some("30.seconds")));
    let plain = check_source("model Presence do\n  attribute :room, :string\nend\n").unwrap().remove(0);
    assert_eq!((plain.storage, plain.expire_after), (Storage::Disk, None));
}

#[test]
fn apply_values_build_the_ephemeral_values_of_an_event() {
    let model = check_source(
        "model Sample do\n  attribute :name, :string\n  ephemeral do\n    attribute :typing, :bool, default: false, reset_after: 5.seconds\n  end\n  scope :one do\n    query \"One\"\n    subscribe \"Typing\", apply: { effect: :upsert, values: { typing: true } }\n  end\nend\n",
    )
    .unwrap()
    .remove(0);
    let updates = model.relations[0].updates[0].clone();
    assert_eq!(updates.set.get("typing"), Some(&json!(true)));
}

#[test]
fn a_delete_effect_sets_the_delete_action() {
    let model = build_one("  scope :feed do\n    query \"Feed\"\n    subscribe \"Gone\", apply: { effect: :delete }, debounce: 300.milliseconds\n  end");
    let update = &model.relations[0].updates[0];
    assert_eq!(update.apply.as_ref().map(|apply| &apply.action), Some(&dblayer::schema::SubscriptionAction::Delete));
    assert_eq!(update.debounce.as_deref(), Some("300.milliseconds"));
}

#[test]
fn variables_rename_parameters_and_fix_constants() {
    let model = build_one("  scope :list do\n    parameter :status, :string\n    query \"items\"\n    variables statusFilter: :status, includeGifts: false, mediaKinds: [\"image\", \"video\"]\n  end");
    let variables = &model.relations[0].remote.as_ref().unwrap().variables;
    assert_eq!(variables["statusFilter"], VariableSource::Param { name: "status".to_string() });
    assert_eq!(variables["includeGifts"], VariableSource::Constant { value: json!(false) });
    assert_eq!(variables["mediaKinds"], VariableSource::Constant { value: json!(["image", "video"]) });
}

#[test]
fn a_scope_can_follow_several_subscriptions() {
    let model = build_one("  scope :list do\n    query \"items\"\n    subscribe \"itemCreated\"\n    subscribe \"itemUpdated\", debounce: 300.milliseconds\n    subscribe \"itemDeleted\", apply: { effect: :delete }\n  end");
    let updates = &model.relations[0].updates;
    assert_eq!(updates.iter().map(|update| update.subscription.as_str()).collect::<Vec<_>>(), ["itemCreated", "itemUpdated", "itemDeleted"]);
    assert_eq!(updates[1].debounce.as_deref(), Some("300.milliseconds"));
    assert_eq!((updates[2].apply.as_ref().map(|apply| &apply.action), updates[0].apply.as_ref().map(|apply| &apply.action)), (Some(&dblayer::schema::SubscriptionAction::Delete), Some(&dblayer::schema::SubscriptionAction::Upsert)));
}

#[test]
fn a_remote_scope_can_take_parameters_that_name_columns() {
    let model = build_one("  scope :by_owner do\n    parameter :ownerId, :id\n    where ownerId: :ownerId\n    query \"owned\"\n    paginate size: 10, direction: :forward\n  end");
    let relation = &model.relations[0];
    assert_eq!(relation.by_columns, vec!["ownerId".to_string()]);
    assert!(!relation.is_global && relation.remote.is_some());
}

#[test]
fn a_scope_can_take_the_id_as_its_column() {
    let model = build_one("  scope :details do\n    parameter :id, :id\n    where id: :id\n    query \"sample\"\n  end");
    assert_eq!(model.relations[0].by_columns, vec!["id".to_string()]);
    assert!(!model.field("id").unwrap().indexed);
    assert!(dblayer::schema::validate_models(&[model], &[]).is_empty());
}

#[test]
fn a_remote_scope_can_order_its_rows_locally() {
    let model = build_one("  scope :recent do\n    order createdAt: :desc\n    query \"recent\"\n  end");
    assert_eq!(model.relations[0].sort, Sort::Fields(vec![dblayer::schema::SortField { column: "createdAt".to_string(), direction: SortDirection::Desc }]));
}

#[test]
fn server_only_parameters_keep_their_types_and_enum_defaults() {
    let model = build_one("  scope :found do\n    parameter :query, :string\n    parameter :limit, :int\n    parameter :includeOld, :bool\n    parameter :list, ListKind, default: \"PRIMARY\"\n    query \"found\"\n    response shape: :list\n  end");
    let relation = &model.relations[0];
    assert_eq!(relation.text_params, vec!["query".to_string(), "limit".to_string(), "includeOld".to_string(), "list".to_string()]);
    assert_eq!(relation.scalar_types.get("limit"), Some(&ScalarKind::Int));
    assert_eq!(relation.scalar_types.get("includeOld"), Some(&ScalarKind::Bool));
    assert_eq!(relation.text_types.get("list").map(String::as_str), Some("ListKind"));
    assert_eq!(relation.text_defaults.get("list").map(String::as_str), Some("PRIMARY"));
}

#[test]
fn refresh_on_foreground_marks_the_relation() {
    let model = build_one("  scope :feed, refresh_on_foreground: true do\n    query \"Feed\"\n  end\n  scope :plain do\n    query \"Plain\"\n  end");
    assert_eq!((model.relations[0].refresh_on_foreground, model.relations[1].refresh_on_foreground), (true, false));
}

#[test]
fn stable_list_merges_an_id_list_and_is_refused_on_a_scalar() {
    let model = build_one("  attribute :momentIds, [:id], merge: :stable_list");
    assert_eq!(model.fields.iter().find(|field| field.name == "momentIds").unwrap().merge, Some(dblayer::schema::MergeRule::StableList));
    let source = "model Sample do\n  attribute :name, :string, merge: :stable_list\nend\n";
    let errors = check_source(source).unwrap_err();
    assert_eq!(errors.iter().map(|error| error.message.as_str()).collect::<Vec<_>>(), ["model Sample: field name has a merge rule that does not fit its type"]);
}

#[test]
fn a_node_scope_reads_one_object_by_the_node_parameter() {
    let model = build_one("  scope :node, node: :one do\n    parameter :id, :id\n    query \"SampleNode\"\n    variables sampleId: :id\n    freshness expires_in: 1.minute\n  end\n  scope :nodes, node: :many do\n    query \"SampleNodes\"\n  end");
    let one = &model.relations[0];
    assert_eq!(one.node, Some(dblayer::schema::NodeRead::One));
    assert_eq!(one.text_params, vec![dblayer::schema::NODE_PARAM.to_string()]);
    assert_eq!(one.remote.as_ref().unwrap().variables["sampleId"], VariableSource::Param { name: dblayer::schema::NODE_PARAM.to_string() });
    assert_eq!(one.remote.as_ref().unwrap().stale_time, "1.minute");
    let many = &model.relations[1];
    assert_eq!((many.node, many.remote.as_ref().unwrap().shape.clone()), (Some(dblayer::schema::NodeRead::Many), RemoteShape::List { coverage: Coverage::Complete }));
}

#[test]
fn an_attribute_merge_rule_keeps_its_words_tiers_and_keys() {
    let models = check_source("model Sample do\n  attribute :media, Media, null: true, merge: { fileUrl: :non_empty, width: :positive, status: [[:PENDING, :PROGRESSING], [:COMPLETED]], progress: :monotonic } do\n    attribute :fileUrl, :string, null: true\n  end\n  attribute :url, :string, null: true, merge: :non_empty\nend\n").unwrap();
    let fields = &models[0].fields;
    let tiers = vec![vec!["PENDING".to_string(), "PROGRESSING".to_string()], vec!["COMPLETED".to_string()]];
    let keys = [("fileUrl", MergeRule::NonEmpty), ("width", MergeRule::Positive), ("status", MergeRule::Ladder { tiers }), ("progress", MergeRule::Monotonic)].map(|(key, rule)| (key.to_string(), rule)).into();
    assert_eq!(fields.iter().find(|field| field.name == "media").unwrap().merge, Some(MergeRule::Keys { keys }));
    assert_eq!(fields.iter().find(|field| field.name == "url").unwrap().merge, Some(MergeRule::NonEmpty));
}

#[test]
fn a_local_attribute_and_a_local_key_become_the_local_merge_rule() {
    let models = check_source("model Sample do\n  attribute :tone, :string, local: true\n  attribute :media, Media, optional: true, merge: { tone: :local, width: :positive } do\n    attribute :width, :int, null: true\n  end\nend\n").unwrap();
    let fields = &models[0].fields;
    assert_eq!(fields.iter().find(|field| field.name == "tone").unwrap().merge, Some(MergeRule::Local));
    let keys = [("tone", MergeRule::Local), ("width", MergeRule::Positive)].map(|(key, rule)| (key.to_string(), rule)).into();
    assert_eq!(fields.iter().find(|field| field.name == "media").unwrap().merge, Some(MergeRule::Keys { keys }));
}

#[test]
fn match_by_keeps_the_correlation_paths_of_a_standard_and_a_named_operation() {
    let models = check_source("model Sample do\n  attribute :body, :string\n  attribute :media, Media, null: true do\n    attribute :blobId, :string, null: true\n  end\n  operation :create do\n    mutation \"CreateSample\"\n    match_by \"body\", \"media.blobId\"\n  end\n  operation :upload, upload: true do\n    mutation \"Upload\"\n    optimistic false\n    match_by \"body\"\n  end\nend\n").unwrap();
    let actions = &models[0].actions;
    assert_eq!(actions.iter().find(|action| action.name == "create").unwrap().correlate, ["body", "media.blobId"]);
    let upload = actions.iter().find(|action| action.name == "upload").unwrap();
    assert_eq!((upload.correlate.clone(), upload.upload), (vec!["body".to_string()], true));
}

#[test]
fn a_scope_order_keeps_the_null_placement_of_each_key() {
    let models = check_source("model Sample do\n  attribute :seq, :int, null: true\n  attribute :createdAt, :date\n  scope :thread do\n    order seq: :asc_nulls_last, createdAt: :desc\n  end\nend\n").unwrap();
    let sort = &models[0].relations.iter().find(|relation| relation.name == "thread").unwrap().sort;
    assert_eq!(*sort, dblayer::schema::Sort::Fields(vec![
        dblayer::schema::SortField { column: "seq".to_string(), direction: SortDirection::AscNullsLast },
        dblayer::schema::SortField { column: "createdAt".to_string(), direction: SortDirection::Desc },
    ]));
}

#[test]
fn a_batch_with_validates_builds_the_upload_batch() {
    let models = check_source("model Sample do\n  attribute :clientKey, :string\n  attribute :group, Group, null: true do\n    attribute :position, :int\n    attribute :size, :int\n  end\n  operation :one, upload: true do\n    mutation \"One\"\n    optimistic false\n    match_by \"clientKey\"\n  end\n  operation :many, upload: true do\n    mutation \"Many\"\n    optimistic false\n    match_by \"clientKey\"\n    batch items: \"items\", single: :one, input: { clientKey: :clientKey }, renumber: { position: \"group.position\", size: \"group.size\", start: 1 }\n    validates :items, length: { minimum: 2, maximum: 10 }\n  end\nend\n").unwrap();
    let batch = models[0].actions[1].batch.clone().unwrap();
    assert_eq!((batch.min, batch.max, batch.items.as_str(), batch.single.as_str()), (2, 10, "items", "one"));
    assert_eq!(batch.input.get("clientKey").map(String::as_str), Some("clientKey"));
    let renumber = batch.renumber.unwrap();
    assert_eq!((renumber.position.as_str(), renumber.size.as_str(), renumber.position_from), ("group.position", "group.size", 1));
}

#[test]
fn preload_keys_values_by_the_scope_param_name_or_the_where_column() {
    let spec = build_one("  scope :by_owner do\n    parameter :owner, :id\n    where ownerId: :owner\n    query \"S\"\n    preload owner: [\"1\", \"2\"]\n  end\n  scope :on_channel do\n    parameter :channel, :string\n    query \"T\"\n    preload channel: [\"a\"]\n  end\n  scope :all do\n    query \"U\"\n    preload\n  end\n  scope :never do\n    query \"V\"\n  end");
    let preload = |name: &str| spec.relations.iter().find(|relation| relation.name == name).unwrap().preload.clone();
    assert_eq!(preload("by_owner"), Some([("ownerId".to_string(), dblayer::schema::PreloadChoice::Listed(vec![json!("1"), json!("2")]))].into()));
    assert_eq!(preload("on_channel"), Some([("channel".to_string(), dblayer::schema::PreloadChoice::Listed(vec![json!("a")]))].into()));
    assert_eq!(preload("all"), Some(Default::default()));
    assert_eq!(preload("never"), None);
}

#[test]
fn preload_takes_the_values_of_a_parameter_from_the_rows_of_another_scope() {
    let spec = build_one("  scope :feed do\n    query \"F\"\n  end\n  scope :by_vibe do\n    parameter :vibeId, :id\n    query \"S\"\n    preload vibeId: { from: \"Sample.feed\" }, limit: 40\n  end");
    let by_vibe = spec.relations.iter().find(|relation| relation.name == "by_vibe").unwrap();
    let source = dblayer::schema::PreloadSource { model: "Sample".to_string(), relation: "feed".to_string() };
    assert_eq!(by_vibe.preload, Some([("vibeId".to_string(), dblayer::schema::PreloadChoice::RowsOf { from: source })].into()));
    assert_eq!(by_vibe.preload_limit, Some(40));
    assert_eq!(by_vibe.preload_order, ["vibeId"]);
}
