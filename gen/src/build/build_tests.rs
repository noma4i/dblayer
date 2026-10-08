use dblayer::schema::{
    ActionMode, AssociationKind, Coverage, Dependent, FieldKind, ModelSpec, PageDirection, RemoteShape, RootShape, Sort,
    UpdatesSpec,
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
    parse_models(&format!("model Sample do\n  schema do\n{lines}\n  end\nend\n")).unwrap().remove(0).fields
}

#[test]
fn empty_source_has_no_models() {
    assert_eq!(parse_models("").unwrap(), vec![]);
    assert_eq!(parse_models("# nothing\n").unwrap(), vec![]);
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
        "model Owner do\n  schema do\n    string :n\n  end\nend\nmodel Sample do\n  schema do\n    id :ownerId, index: true\n    date :createdAt, index: true\n    index :ownerId, :createdAt\n  end\n  belongs_to :owner, model: Owner, key: :ownerId, touch: true\n  has_one :first, model: Sample, key: :ownerId, dependent: :destroy\n  references :peers, model: Owner\nend\n",
    )
    .unwrap()
    .remove(1);
    assert_eq!(model.composite_indexes, vec![vec!["ownerId".to_string(), "createdAt".to_string()]]);
    let owner = &model.associations[0];
    assert_eq!((owner.kind, owner.target.as_str(), owner.foreign_key.as_str(), owner.touches_parent), (AssociationKind::BelongsTo, "Owner", "ownerId", true));
    assert_eq!(model.associations[1].dependent, Some(Dependent::Destroy));
    assert_eq!(model.associations[2].kind, AssociationKind::References);
    assert_eq!(model.associations[2].foreign_key, "");
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
    assert_eq!(remote.shape, RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: Some(5) });
    assert!(remote.selects_page_info);
    assert_eq!(remote.empty_stale_time.as_deref(), Some("30.seconds"));
}

#[test]
fn remote_paginate_backward_without_cap() {
    let model = build_one("  remote :feed do\n    query Feed\n    paginate 20, backward: true\n  end");
    let remote = model.relations[0].remote.as_ref().unwrap();
    assert_eq!(remote.shape, RemoteShape::Connection { direction: PageDirection::Backward, coverage: Coverage::Page, max_pages: None });
}

#[test]
fn scopes_and_remotes_keep_written_order() {
    let model = build_one("  remote :b do\n    query B\n  end\n  scope :a\n  remote :c do\n    query C\n  end");
    let names: Vec<_> = model.relations.iter().map(|relation| relation.name.as_str()).collect();
    assert_eq!(names, vec!["b", "a", "c"]);
}

#[test]
fn comments_and_blank_lines_are_ignored() {
    let model = build_one("  # note\n\n  scope :a # tail\n");
    assert_eq!(model.relations.len(), 1);
}

#[test]
fn standard_actions_bind_mutations_and_input() {
    let model = build_one("  create with: CreateSample, input: { text: :createdAt, ownerId: :ownerId }\n  update with: UpdateSample\n  destroy with: DeleteSample");
    let names: Vec<_> = model.actions.iter().map(|action| action.name.as_str()).collect();
    assert_eq!(names, vec!["create", "update", "destroy"]);
    let create = &model.actions[0];
    assert_eq!((create.mutation.as_deref(), create.mode, create.root_shape), (Some("CreateSample"), ActionMode::Durable, RootShape::Upsert));
    assert!(create.has_optimistic && create.has_temp_row_retention);
    assert_eq!(create.variables.get("text").map(String::as_str), Some("createdAt"));
    assert_eq!(model.actions[1].variables.len(), 0);
    let destroy = &model.actions[2];
    assert_eq!((destroy.mutation.as_deref(), destroy.mode, destroy.root_shape), (Some("DeleteSample"), ActionMode::Request, RootShape::Destroy));
}

#[test]
fn action_block_collects_mutation_input_optimistic_and_refetch() {
    let model = build_one("  remote :feed do\n    query Feed\n  end\n  action :like do\n    mutation LikeSample\n    input sampleId: :id\n    optimistic createdAt: \"liked\", ownerId: nil\n    refetch :feed\n  end");
    let action = &model.actions[0];
    assert_eq!(action.name, "like");
    assert_eq!((action.mode, action.root_shape, action.has_optimistic), (ActionMode::Request, RootShape::Upsert, false));
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
    assert_eq!(updates(0), Some(UpdatesSpec { subscription: "SampleEvents".into(), deleted_by: Some("destroyed".into()) }));
    assert_eq!(updates(1), Some(UpdatesSpec { subscription: "PlainEvents".into(), deleted_by: None }));
}
