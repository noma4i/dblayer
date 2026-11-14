use super::relation_schema_tests::action;
use super::*;
use serde_json::json;

pub(in super::super) fn object(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

pub(in super::super) fn id_field() -> FieldSpec {
    FieldSpec::raw("id")
}

pub(in super::super) fn model(fields: Vec<FieldSpec>) -> ModelSpec {
    ModelSpec::new("messages", fields)
}

pub(in super::super) fn paged_remote(selects_page_info: bool) -> RemoteSpec {
    RemoteSpec {
        shape: RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None },
        operation: "PagedOperation".into(),
        stale_time: "paged_session".into(),
        empty_stale_time: None,
        selects_page_info,
        page_size: None,
        variables: Default::default(), root: None, total_from: None,
    }
}

pub(in super::super) fn complete_remote() -> RemoteSpec {
    RemoteSpec {
        shape: RemoteShape::List { coverage: Coverage::Complete },
        operation: "CatalogOperation".into(),
        stale_time: "catalog".into(),
        empty_stale_time: Some("empty_retry".into()),
        selects_page_info: false,
        page_size: None,
        variables: Default::default(),
        root: None,
        total_from: None,
    }
}

pub(in super::super) fn association(name: &str, kind: AssociationKind, target: &str, foreign_key: &str) -> AssociationSpec {
    AssociationSpec::new(name, kind, target, foreign_key)
}

#[test]
fn normalize_reads_rejects_defaults_and_nulls() {
    let plain = model(vec![id_field()]);
    let defaulted = model(vec![id_field().with_default(json!("x"))]);
    let nullable = model(vec![id_field().nullable()]);
    let undecodable = model(vec![FieldSpec::object("id").nullable()]);
    let optional = model(vec![id_field().optional()]);
    let optional_nullable = model(vec![id_field().optional().nullable()]);

    assert_eq!(plain.normalize(&object(json!({"id": "7"}))).unwrap(), object(json!({"id": "7"})));
    assert_eq!(plain.normalize(&Map::new()), Err(SchemaError::MissingField("id".into())));
    assert_eq!(defaulted.normalize(&Map::new()).unwrap(), object(json!({"id": "x"})));
    assert_eq!(nullable.normalize(&Map::new()).unwrap(), object(json!({"id": null})));
    assert_eq!(undecodable.normalize(&object(json!({"id": "7"}))).unwrap(), object(json!({"id": null})));
    assert_eq!(optional.normalize(&Map::new()).unwrap(), Map::new());
    assert_eq!(optional.normalize(&object(json!({"id": "7"}))).unwrap(), object(json!({"id": "7"})));
    assert_eq!(optional_nullable.normalize(&Map::new()).unwrap(), Map::new());
    assert_eq!(optional_nullable.normalize(&object(json!({"id": null}))).unwrap(), object(json!({"id": null})));
}

#[test]
fn patch_keeps_present_values_and_indexed_names() {
    let kind = FieldSpec::raw("kind").optional().indexed();
    let spec = model(vec![id_field(), kind]);

    let value_patch = spec.patch(&object(json!({"id": "7"})));
    let null_patch = spec.patch(&object(json!({"id": null})));

    assert_eq!(value_patch.len(), 1);
    assert_eq!(value_patch["id"], json!("7"));
    assert_eq!(null_patch, object(json!({"id": null})));
    assert_eq!(spec.indexed_names(), vec!["kind".to_string()]);
}

#[test]
fn list_object_array_and_enum_fields_keep_only_valid_shapes() {
    let list = FieldSpec::list("counts", ScalarKind::Int);
    let record = FieldSpec::object("metadata");
    let array = FieldSpec::array("values");
    let media = FieldSpec::enumeration("media", &["image", "video"]);
    let object_value = json!({"enabled": true});
    let array_value = json!(["first", 2]);

    assert_eq!(list.decode(&json!([1, "3"])), Some(json!([1, 3])));
    assert_eq!(list.decode(&json!([1, "x", "3"])), None);
    assert_eq!(record.decode(&object_value), Some(object_value.clone()));
    assert_eq!(record.decode(&json!("metadata")), None);
    assert_eq!(array.decode(&array_value), Some(array_value.clone()));
    assert_eq!(array.decode(&json!(2)), None);
    assert_eq!(media.decode(&json!("image")), Some(json!("image")));
    assert_eq!(media.decode(&json!("audio")), Some(json!("audio")));
    assert_eq!(media.decode(&json!(3)), None);
    assert_eq!(FieldSpec::raw("any").decode(&Value::Null), None);
}

#[test]
fn parent_id_field_reads_direct_value_then_parent_id() {
    let field = FieldSpec::parent_id("authorId", "author");
    let parent = json!({"id": "parent-id"});

    assert_eq!(field.read(&object(json!({"authorId": "direct-id", "author": parent.clone()}))), Some(json!("direct-id")));
    assert_eq!(field.read(&object(json!({"author": parent}))), Some(json!("parent-id")));
    assert_eq!(field.read(&object(json!({"other": "value"}))), None);
    assert_eq!(field.decode(&json!("7")), Some(json!("7")));
}

#[test]
fn merged_keeps_stored_fields_and_creates_with_defaults() {
    let spec = model(vec![
        FieldSpec::scalar("id", ScalarKind::Str),
        FieldSpec::scalar("kind", ScalarKind::Str).with_default(json!("text")),
        FieldSpec::scalar("body", ScalarKind::Str),
    ]);
    let stored = object(json!({"id": "1", "kind": "voice", "body": "hi"}));

    assert_eq!(spec.merged(Some(&stored), &object(json!({"id": "1", "body": "yo"}))), Some(object(json!({"id": "1", "kind": "voice", "body": "yo"}))));
    assert_eq!(spec.merged(None, &object(json!({"id": "1", "body": "yo"}))), Some(object(json!({"id": "1", "kind": "text", "body": "yo"}))));
    assert_eq!(spec.merged(None, &object(json!({"id": "1"}))), None);
}

#[test]
fn model_reports_invalid_key_fields_and_names() {
    let empty_key = ModelSpec::new("", vec![id_field()]);
    let no_fields = model(vec![]);
    let duplicate = model(vec![id_field(), id_field()]);
    let reserved = model(vec![FieldSpec::scalar("orderKey", ScalarKind::Str)]);
    let dollar = model(vec![FieldSpec::scalar("$meta", ScalarKind::Str)]);
    let empty_name = model(vec![FieldSpec::scalar("", ScalarKind::Str)]);

    assert_eq!(empty_key.validate(&[]), Err(DeclarationError::EmptyModelKey));
    assert_eq!(no_fields.validate(&[]), Err(DeclarationError::NoFields));
    assert_eq!(duplicate.validate(&[]), Err(DeclarationError::DuplicateField("id".into())));
    assert_eq!(reserved.validate(&[]), Err(DeclarationError::ReservedFieldName("orderKey".into())));
    assert_eq!(dollar.validate(&[]), Err(DeclarationError::ReservedFieldName("$meta".into())));
    assert_eq!(empty_name.validate(&[]), Err(DeclarationError::EmptyFieldName));
}

#[test]
fn indexed_fields_must_be_scalar_and_map_to_distinct_columns() {
    let object_index = model(vec![FieldSpec::object("media").indexed()]);
    let service_column = model(vec![FieldSpec::scalar("born_rev", ScalarKind::Str).indexed()]);
    let id_column = model(vec![FieldSpec::scalar("id", ScalarKind::Id).indexed()]);
    let same_column = model(vec![
        FieldSpec::scalar("chatId", ScalarKind::Id).indexed(),
        FieldSpec::scalar("chat_id", ScalarKind::Id).indexed(),
    ]);
    let fine = model(vec![FieldSpec::scalar("chatId", ScalarKind::Id).indexed(), FieldSpec::enumeration("kind", &["a"]).indexed()]);

    assert_eq!(object_index.validate(&[]), Err(DeclarationError::IndexedFieldNotScalar("media".into())));
    assert_eq!(service_column.validate(&[]), Err(DeclarationError::ColumnCollision("born_rev".into())));
    assert_eq!(id_column.validate(&[]), Err(DeclarationError::ColumnCollision("id".into())));
    assert_eq!(same_column.validate(&[]), Err(DeclarationError::ColumnCollision("chat_id".into())));
    assert_eq!(fine.validate(&[]), Ok(()));
}

#[test]
fn indexes_accept_scalar_columns_without_separate_field_indexes() {
    let fields = vec![
        FieldSpec::scalar("chatId", ScalarKind::Id).indexed(),
        FieldSpec::scalar("sequenceNumber", ScalarKind::Int).indexed(),
        FieldSpec::scalar("body", ScalarKind::Str),
    ];
    let with = |composite: Vec<&str>| ModelSpec {
        indexes: vec![composite.into_iter().map(String::from).collect()],
        ..model(fields.clone())
    };

    assert_eq!(with(vec!["chatId", "sequenceNumber"]).validate(&[]), Ok(()));
    assert_eq!(with(vec!["chatId"]).validate(&[]), Ok(()));
    assert_eq!(with(vec!["chatId", "ghost"]).validate(&[]), Err(DeclarationError::InvalidIndexColumn("ghost".into())));
    assert_eq!(with(vec!["chatId", "body"]).validate(&[]), Ok(()));
}

#[test]
fn model_accepts_indexed_association_and_reports_missing_index() {
    let fields = vec![id_field(), FieldSpec::scalar("chatId", ScalarKind::Id).indexed()];
    let accepted = ModelSpec {
        associations: vec![association("chat", AssociationKind::BelongsTo, "chats", "chatId")],
        ..model(fields.clone())
    };
    let rejected = ModelSpec {
        associations: vec![association("author", AssociationKind::BelongsTo, "chats", "authorId")],
        ..model(fields)
    };

    assert_eq!(accepted.indexed_names(), vec!["chatId".to_string()]);
    assert_eq!(accepted.validate(&[]), Ok(()));
    assert_eq!(rejected.validate(&[]), Err(DeclarationError::ForeignKeyNotIndexed("author".into())));
}

#[test]
fn references_key_must_be_a_declared_field() {
    let undeclared = ModelSpec { associations: vec![association("chats", AssociationKind::References, "chats", "chatIds")], ..model(vec![id_field()]) };
    assert_eq!(undeclared.validate(&[]), Err(DeclarationError::ReferencesKeyNotField("chats".into())));
    let declared = ModelSpec { associations: vec![association("chats", AssociationKind::References, "chats", "id")], ..model(vec![id_field()]) };
    assert_eq!(declared.validate(&[]), Ok(()));
}

#[test]
fn association_kinds_choose_ownership() {
    let belongs_to = association("chat", AssociationKind::BelongsTo, "chats", "chatId");
    let has_many = association("messages", AssociationKind::HasMany, "chats", "chatId");
    let references = association("chats", AssociationKind::References, "chats", "");

    assert!(!belongs_to.owns_children());
    assert!(belongs_to.requires_foreign_key());
    assert!(has_many.owns_children());
    assert!(!references.requires_foreign_key());

    let host = ModelSpec { associations: vec![references], ..model(vec![id_field()]) };
    assert_eq!(host.validate(&[]), Ok(()));
}

#[test]
fn association_faults_for_foreign_key_and_dependent() {
    let host = |association: AssociationSpec| ModelSpec {
        associations: vec![association],
        ..model(vec![id_field(), FieldSpec::scalar("chatId", ScalarKind::Id).indexed()])
    };
    let with_dependent = |kind| AssociationSpec { dependent: Some(Dependent::Destroy), ..association("a", kind, "chats", "chatId") };

    assert_eq!(
        host(association("a", AssociationKind::HasMany, "chats", "")).validate(&[]),
        Err(DeclarationError::EmptyForeignKey("a".into()))
    );
    assert_eq!(
        host(with_dependent(AssociationKind::BelongsTo)).validate(&[]),
        Err(DeclarationError::DependentOnNonOwner("a".into()))
    );
    assert_eq!(host(with_dependent(AssociationKind::HasMany)).validate(&[]), Ok(()));
}

#[test]
fn an_action_that_lands_an_unknown_model_is_reported() {
    let chats = ModelSpec::new("chats", vec![id_field()]);
    let mut landing = action("read", RootShape::Upsert);
    landing.lands.push(ActionLanding { model: "ghosts".into(), root: "chatRead.ghost".into() });
    let host = ModelSpec { actions: vec![landing.clone()], ..ModelSpec::new("moments", vec![id_field()]) };
    assert_eq!(
        validate_models(&[chats.clone(), host], &[]),
        vec![ModelFault { model: ModelKey::new("moments"), error: DeclarationError::UnknownLanding { action: "read".into(), model: "ghosts".into() } }]
    );
    landing.lands[0].model = "chats".into();
    let host = ModelSpec { actions: vec![landing], ..ModelSpec::new("moments", vec![id_field()]) };
    assert!(validate_models(&[chats, host], &[]).is_empty());
}

#[test]
fn a_declared_field_link_needs_a_known_model_and_an_id_attribute_beside_it() {
    let users = ModelSpec::new("users", vec![id_field()]);
    let host = |link: FieldLink| {
        let element = FieldSpec {
            fields: vec![FieldSpec::scalar("userId", ScalarKind::Id).nullable(), FieldSpec { link: Some(link), ..FieldSpec::object("user").nullable() }],
            ..FieldSpec::array("reactions").optional()
        };
        ModelSpec::new("messages", vec![id_field(), element])
    };
    let link = |model: &str, id: &str| FieldLink { model: model.into(), id: id.into() };
    assert!(validate_models(&[users.clone(), host(link("users", "userId"))], &[]).is_empty());
    assert_eq!(validate_models(&[users.clone(), host(link("ghosts", "userId"))], &[])[0].error, DeclarationError::UnknownFieldLink("user".into()));
    assert_eq!(validate_models(&[users, host(link("users", "missing"))], &[])[0].error, DeclarationError::FieldLinkWithoutId("user".into()));
}

#[test]
fn cross_model_checks_report_unknown_target_and_foreign_key() {
    let chats = ModelSpec::new("chats", vec![id_field(), FieldSpec::scalar("unreadCount", ScalarKind::Int)]);
    let messages = |association: AssociationSpec| ModelSpec {
        associations: vec![association],
        ..ModelSpec::new("messages", vec![id_field(), FieldSpec::scalar("chatId", ScalarKind::Id).indexed()])
    };
    let plain = association("chat", AssociationKind::BelongsTo, "chats", "chatId");
    let unknown = association("chat", AssociationKind::BelongsTo, "ghosts", "chatId");
    let owner = association("messages", AssociationKind::HasMany, "messages", "chatId");
    let bad_owner = association("messages", AssociationKind::HasMany, "messages", "authorId");
    let owner_host = |association: AssociationSpec| ModelSpec { associations: vec![association], ..chats.clone() };

    assert!(validate_models(&[chats.clone(), messages(plain)], &[]).is_empty());
    assert_eq!(
        validate_models(&[chats.clone(), messages(unknown)], &[]),
        vec![ModelFault { model: ModelKey::new("messages"), error: DeclarationError::UnknownTarget("chat".into()) }]
    );
    let messages_plain = messages(association("chat", AssociationKind::BelongsTo, "chats", "chatId"));
    assert!(validate_models(&[owner_host(owner), messages_plain.clone()], &[]).is_empty());
    assert_eq!(
        validate_models(&[owner_host(bad_owner), messages_plain], &[])[0].error,
        DeclarationError::ForeignKeyNotIndexed("messages".into())
    );
}

#[test]
fn local_and_global_relations_are_local_without_remote() {
    let local = RelationSpec::new("thread", &["chatId"]);
    let global = RelationSpec::global("catalog");
    let non_global_empty = RelationSpec::new("catalog", &[]);

    assert!(local.is_local());
    assert!(!local.has_more());
    assert!(global.by_columns.is_empty());
    assert!(global.is_global);
    assert!(global.is_local());
    assert!(!non_global_empty.is_global);
    let key = local.scope_key("messages", &[("chatId".to_string(), crate::value::relation_param_text(&json!("7")))].into_iter().collect()).unwrap();
    assert!(key.as_str().contains("messages"));
    assert!(key.as_str().contains("thread"));
}

#[test]
fn remote_relation_follows_paging() {
    let paged = RelationSpec { remote: Some(paged_remote(true)), ..RelationSpec::new("thread", &["chatId"]) };
    let global = RelationSpec { remote: Some(complete_remote()), ..RelationSpec::global("catalog") };

    assert!(!paged.is_local());
    assert!(paged.has_more());
    assert!(!global.is_local());
    assert!(!global.has_more());
    assert_eq!(paged.remote.as_ref().unwrap().coverage(), Coverage::Page);
    assert_eq!(global.remote.as_ref().unwrap().coverage(), Coverage::Complete);
    assert_eq!(RemoteSpec { shape: RemoteShape::Single, ..complete_remote() }.coverage(), Coverage::Complete);
}
