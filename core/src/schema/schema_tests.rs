use super::*;
use serde_json::json;

fn object(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

fn id_field() -> FieldSpec {
    FieldSpec::raw("id")
}

fn model(fields: Vec<FieldSpec>) -> ModelSpec {
    ModelSpec::new("messages", fields)
}

fn paged_remote(selects_page_info: bool) -> RemoteSpec {
    RemoteSpec {
        shape: RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None },
        operation: "PagedOperation".into(),
        stale_time: "paged_session".into(),
        empty_stale_time: None,
        selects_page_info,
        updates: Vec::new(),
        page_size: None,
        variables: Default::default(), root: None,
    }
}

fn complete_remote() -> RemoteSpec {
    RemoteSpec {
        shape: RemoteShape::List { coverage: Coverage::Complete },
        operation: "CatalogOperation".into(),
        stale_time: "catalog".into(),
        empty_stale_time: Some("empty_retry".into()),
        selects_page_info: false,
        updates: Vec::new(),
        page_size: None,
        variables: Default::default(),
        root: None,
    }
}

fn association(name: &str, kind: AssociationKind, target: &str, foreign_key: &str) -> AssociationSpec {
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

    assert_eq!(list.decode(&json!([1, "x", "3"])), Some(json!([1, 3])));
    assert_eq!(record.decode(&object_value), Some(object_value.clone()));
    assert_eq!(record.decode(&json!("metadata")), None);
    assert_eq!(array.decode(&array_value), Some(array_value.clone()));
    assert_eq!(array.decode(&json!(2)), None);
    assert_eq!(media.decode(&json!("image")), Some(json!("image")));
    assert_eq!(media.decode(&json!("audio")), None);
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
fn normalize_all_keeps_valid_rows_and_reports_dropped() {
    let spec = model(vec![FieldSpec::scalar("id", ScalarKind::Str)]);
    let inputs = vec![object(json!({"id": "1"})), Map::new(), object(json!({"id": "3"}))];

    let (rows, dropped) = spec.normalize_all(&inputs);
    let (none, none_dropped) = spec.normalize_all(&[]);

    assert_eq!(rows, vec![object(json!({"id": "1"})), object(json!({"id": "3"}))]);
    assert_eq!(dropped, vec![(1, SchemaError::MissingField("id".into()))]);
    assert!(none.is_empty());
    assert!(none_dropped.is_empty());
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
    let service_column = model(vec![FieldSpec::scalar("payload", ScalarKind::Str).indexed()]);
    let id_column = model(vec![FieldSpec::scalar("id", ScalarKind::Id).indexed()]);
    let same_column = model(vec![
        FieldSpec::scalar("chatId", ScalarKind::Id).indexed(),
        FieldSpec::scalar("chat_id", ScalarKind::Id).indexed(),
    ]);
    let fine = model(vec![FieldSpec::scalar("chatId", ScalarKind::Id).indexed(), FieldSpec::enumeration("kind", &["a"]).indexed()]);

    assert_eq!(object_index.validate(&[]), Err(DeclarationError::IndexedFieldNotScalar("media".into())));
    assert_eq!(service_column.validate(&[]), Err(DeclarationError::ColumnCollision("payload".into())));
    assert_eq!(id_column.validate(&[]), Err(DeclarationError::ColumnCollision("id".into())));
    assert_eq!(same_column.validate(&[]), Err(DeclarationError::ColumnCollision("chat_id".into())));
    assert_eq!(fine.validate(&[]), Ok(()));
}

#[test]
fn composite_indexes_need_two_indexed_fields() {
    let fields = vec![
        FieldSpec::scalar("chatId", ScalarKind::Id).indexed(),
        FieldSpec::scalar("sequenceNumber", ScalarKind::Int).indexed(),
        FieldSpec::scalar("body", ScalarKind::Str),
    ];
    let with = |composite: Vec<&str>| ModelSpec {
        composite_indexes: vec![composite.into_iter().map(String::from).collect()],
        ..model(fields.clone())
    };

    assert_eq!(with(vec!["chatId", "sequenceNumber"]).validate(&[]), Ok(()));
    assert_eq!(with(vec!["chatId"]).validate(&[]), Err(DeclarationError::CompositeIndexTooShort));
    assert_eq!(with(vec!["chatId", "ghost"]).validate(&[]), Err(DeclarationError::CompositeIndexFieldNotIndexed("ghost".into())));
    assert_eq!(with(vec!["chatId", "body"]).validate(&[]), Err(DeclarationError::CompositeIndexFieldNotIndexed("body".into())));
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
fn association_kinds_choose_ownership_and_read_plan() {
    let belongs_to = association("chat", AssociationKind::BelongsTo, "chats", "chatId");
    let has_many = association("messages", AssociationKind::HasMany, "chats", "chatId");
    let has_one = association("last", AssociationKind::HasOne, "chats", "chatId");
    let references = association("chats", AssociationKind::References, "chats", "");

    assert!(!belongs_to.owns_children());
    assert!(belongs_to.requires_foreign_key());
    assert_eq!(belongs_to.read_plan(), ReadPlan::Find);
    assert!(has_many.owns_children());
    assert_eq!(has_many.read_plan(), ReadPlan::Where);
    assert_eq!(has_one.read_plan(), ReadPlan::Find);
    assert!(!references.requires_foreign_key());
    assert_eq!(references.read_plan(), ReadPlan::ByIds);

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
    let key = local.scope_key("messages", &[("chatId".to_string(), "7".to_string())].into_iter().collect()).unwrap();
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

fn relation_host(relation: RelationSpec) -> ModelSpec {
    ModelSpec {
        relations: vec![relation],
        ..ModelSpec::new(
            "messages",
            vec![id_field(), FieldSpec::scalar("chatId", ScalarKind::Id).indexed(), FieldSpec::raw("body")],
        )
    }
}

#[test]
fn relation_name_by_columns_and_remote_faults_match_spec() {
    assert_eq!(relation_host(RelationSpec::new("thread", &["chatId"])).validate(&[]), Ok(()));
    assert_eq!(relation_host(RelationSpec::global("catalog")).validate(&[]), Ok(()));
    assert_eq!(
        relation_host(RelationSpec::new("", &["chatId"])).validate(&[]),
        Err(DeclarationError::EmptyRelationName)
    );
    assert_eq!(
        relation_host(RelationSpec::new("$id", &["chatId"])).validate(&[]),
        Err(DeclarationError::ReservedRelationName("$id".into()))
    );
    assert_eq!(
        relation_host(RelationSpec::new("orderKey", &["chatId"])).validate(&[]),
        Err(DeclarationError::ReservedRelationName("orderKey".into()))
    );
    assert_eq!(
        relation_host(RelationSpec::new("thread", &[])).validate(&[]),
        Err(DeclarationError::EmptyByColumns("thread".into()))
    );
    let broken = RelationSpec { remote: Some(paged_remote(false)), ..RelationSpec::new("thread", &["chatId"]) };
    assert_eq!(
        relation_host(broken).validate(&[]),
        Err(DeclarationError::Remote { relation: "thread".into(), fault: RemoteFault::ConnectionWithoutPageInfo })
    );
    let single = RelationSpec {
        remote: Some(RemoteSpec { shape: RemoteShape::Single, selects_page_info: true, ..complete_remote() }),
        ..RelationSpec::global("one")
    };
    assert_eq!(
        relation_host(single).validate(&[]),
        Err(DeclarationError::Remote { relation: "one".into(), fault: RemoteFault::SingleHasPaging })
    );
    let zero_pages = RelationSpec {
        remote: Some(RemoteSpec {
            shape: RemoteShape::Connection { direction: PageDirection::Backward, coverage: Coverage::Page, max_pages: Some(0), cursor: None },
            ..paged_remote(true)
        }),
        ..RelationSpec::new("thread", &["chatId"])
    };
    assert_eq!(
        relation_host(zero_pages).validate(&[]),
        Err(DeclarationError::Remote { relation: "thread".into(), fault: RemoteFault::MaxPagesNotPositive })
    );
}

#[test]
fn relation_columns_must_exist_and_be_indexed() {
    assert_eq!(relation_host(RelationSpec::new("thread", &["chatId"])).validate(&[]), Ok(()));
    assert_eq!(
        relation_host(RelationSpec::new("thread", &["body"])).validate(&[]),
        Err(DeclarationError::IndexMissing { relation: "thread".into(), column: "body".into() })
    );
    assert_eq!(
        relation_host(RelationSpec::new("thread", &["ghost"])).validate(&[]),
        Err(DeclarationError::UnknownByColumn { relation: "thread".into(), column: "ghost".into() })
    );
    let sorted = RelationSpec {
        sort: Sort::Fields(vec![SortField { column: "ghost".into(), direction: SortDirection::Desc }]),
        ..RelationSpec::new("thread", &["chatId"])
    };
    assert_eq!(
        relation_host(sorted).validate(&[]),
        Err(DeclarationError::UnknownSortColumn { relation: "thread".into(), column: "ghost".into() })
    );
}

#[test]
fn relation_names_must_be_unique_and_not_collide() {
    let two = |first: &str, second: &str| ModelSpec {
        relations: vec![RelationSpec::new(first, &["chatId"]), RelationSpec::new(second, &["chatId"])],
        ..relation_host(RelationSpec::new("x", &["chatId"]))
    };
    assert_eq!(two("thread", "thread").validate(&[]), Err(DeclarationError::DuplicateRelation("thread".into())));
    assert_eq!(two("thread", "other").validate(&[]), Ok(()));

    let colliding = ModelSpec {
        associations: vec![association("chat", AssociationKind::References, "chats", "")],
        ..relation_host(RelationSpec::new("chat", &["chatId"]))
    };
    assert_eq!(colliding.validate(&[]), Err(DeclarationError::AssociationNameCollision("chat".into())));

    assert_eq!(
        relation_host(RelationSpec::new("find", &["chatId"])).validate(&["find"]),
        Err(DeclarationError::NameCollision("find".into()))
    );
    let member_clash = ModelSpec {
        associations: vec![association("find", AssociationKind::References, "chats", "")],
        ..relation_host(RelationSpec::new("thread", &["chatId"]))
    };
    assert_eq!(member_clash.validate(&["find"]), Err(DeclarationError::NameCollision("find".into())));
    let clean = ModelSpec {
        associations: vec![association("author", AssociationKind::References, "chats", "")],
        ..relation_host(RelationSpec::new("thread", &["chatId"]))
    };
    assert_eq!(clean.validate(&["find"]), Ok(()));
}

fn action(name: &str, root_shape: RootShape) -> ActionDeclaration {
    ActionDeclaration::new(name, root_shape)
}

fn with_actions(actions: Vec<ActionDeclaration>) -> ModelSpec {
    ModelSpec { actions, ..model(vec![id_field()]) }
}

#[test]
fn action_rules_reject_invalid_combinations() {
    let collision = action("send", RootShape::Upsert);
    let optimistic = ActionDeclaration { has_optimistic: true, ..action("create", RootShape::Upsert) };
    let retained = ActionDeclaration { has_temp_row_retention: true, ..optimistic.clone() };
    let cascade = ActionDeclaration {
        has_optimistic: true,
        has_cascade_dependents: true,
        ..action("remove", RootShape::Destroy)
    };
    let no_cascade = ActionDeclaration { has_cascade_dependents: false, ..cascade.clone() };
    let valid = action("save", RootShape::Upsert);

    assert_eq!(with_actions(vec![collision]).validate(&["send"]), Err(DeclarationError::ActionNameCollision("send".into())));
    assert_eq!(with_actions(vec![optimistic]).validate(&[]), Err(DeclarationError::MissingTempRowRetention("create".into())));
    assert_eq!(with_actions(vec![retained]).validate(&[]), Ok(()));
    assert_eq!(with_actions(vec![cascade]).validate(&[]), Err(DeclarationError::OptimisticDestroyWithCascade("remove".into())));
    assert_eq!(with_actions(vec![no_cascade]).validate(&[]), Ok(()));
    assert_eq!(with_actions(vec![valid.clone()]).validate(&[]), Ok(()));
    assert_eq!(
        with_actions(vec![valid.clone(), valid]).validate(&[]),
        Err(DeclarationError::ActionNameCollision("save".into()))
    );
}

#[test]
fn a_destroying_action_cannot_declare_keep() {
    let mut destroy = action("destroy", RootShape::Destroy);
    destroy.on_failure = OnFailure::Keep;
    let mut upsert = action("update", RootShape::Upsert);
    upsert.on_failure = OnFailure::Keep;
    assert_eq!(with_actions(vec![destroy]).validate(&[]), Err(DeclarationError::KeepOnDestroy("destroy".into())));
    assert_eq!(with_actions(vec![upsert]).validate(&[]), Ok(()));
}

#[test]
fn forbidden_members_and_once_key_are_checked() {
    let with_member = |member: &str| ActionDeclaration {
        members: vec![member.to_string()],
        ..action("save", RootShape::Upsert)
    };
    let once = |dedupe_key: Option<&str>| ActionDeclaration {
        has_once: true,
        dedupe_key: dedupe_key.map(String::from),
        ..action("save", RootShape::Upsert)
    };

    assert_eq!(with_actions(vec![with_member("after")]).validate(&[]), Err(DeclarationError::ForbiddenMember("after".into())));
    assert_eq!(with_actions(vec![with_member("resume")]).validate(&[]), Err(DeclarationError::ForbiddenMember("resume".into())));
    assert_eq!(with_actions(vec![with_member("commit")]).validate(&[]), Ok(()));
    assert_eq!(with_actions(vec![once(None)]).validate(&[]), Err(DeclarationError::OnceKeyRequired("save".into())));
    assert_eq!(with_actions(vec![once(Some(""))]).validate(&[]), Err(DeclarationError::OnceKeyRequired("save".into())));
    assert_eq!(with_actions(vec![once(Some("key"))]).validate(&[]), Ok(()));
}

#[test]
fn specs_round_trip_through_json_and_default_missing_parts() {
    let spec = ModelSpec {
        associations: vec![association("chat", AssociationKind::BelongsTo, "chats", "chatId")],
        relations: vec![RelationSpec { remote: Some(paged_remote(true)), ..RelationSpec::new("thread", &["chatId"]) }],
        composite_indexes: vec![vec!["chatId".into(), "sequenceNumber".into()]],
        actions: vec![action("send", RootShape::Upsert)],
        ..model(vec![
            FieldSpec::scalar("chatId", ScalarKind::Id).indexed(),
            FieldSpec::scalar("sequenceNumber", ScalarKind::Int).nullable().indexed(),
            FieldSpec::list("tags", ScalarKind::Str).with_default(json!([])),
        ])
    };

    let text = serde_json::to_string(&spec).unwrap();
    let decoded: ModelSpec = serde_json::from_str(&text).unwrap();

    assert_eq!(decoded, spec);
    let minimal: ModelSpec = serde_json::from_value(json!({
        "key": "m",
        "fields": [{"name": "id", "kind": {"type": "raw"}}]
    }))
    .unwrap();
    assert_eq!(minimal, ModelSpec::new("m", vec![FieldSpec::raw("id")]));

    let remote = RemoteSpec { updates: vec![UpdatesSpec { subscription: "MomentEvents".into(), deleted_by: Some("destroyed".into()), set: Map::new(), removes: false, debounce: None, root: None, id: None }], ..complete_remote() };
    let text = serde_json::to_string(&remote).unwrap();
    assert_eq!(serde_json::from_str::<RemoteSpec>(&text).unwrap(), remote);
    let bare = r#"{"operation":"Q","shape":{"type":"single"},"stale_time":"5.minutes"}"#;
    assert_eq!(serde_json::from_str::<RemoteSpec>(bare).unwrap().updates, Vec::new());

    let bare = r#"{"name":"like","mode":"request","root_shape":"upsert"}"#;
    let parsed: ActionDeclaration = serde_json::from_str(bare).unwrap();
    assert_eq!((parsed.mutation.clone(), parsed.variables.len(), parsed.optimistic.len(), parsed.refetch.len()), (None, 0, 0, 0));
    let mut full = action("like", RootShape::Upsert);
    full.mutation = Some("LikeMoment".into());
    full.variables.insert("momentId".into(), "id".into());
    full.optimistic.insert("likedByMe".into(), serde_json::json!(true));
    full.refetch.push("feed".into());
    assert_eq!(serde_json::from_str::<ActionDeclaration>(&serde_json::to_string(&full).unwrap()).unwrap(), full);
}

#[test]
fn file_field_decodes_only_a_descriptor_and_cannot_be_indexed() {
    let field = FieldSpec::file("avatar");
    let descriptor = json!({"id": "f1", "name": "a.png", "mime": "image/png", "size": 12, "sha256": "ab"});

    assert_eq!(field.decode(&descriptor), Some(descriptor.clone()));
    assert_eq!(field.decode(&json!({"id": "f1"})), None);
    assert_eq!(field.decode(&json!("f1")), None);
    assert_eq!(field.decode(&json!({"id": 1, "size": 12})), None);
    assert!(column_type(&field.kind).is_none());
    assert_eq!(
        model(vec![id_field(), FieldSpec::file("avatar").indexed()]).validate(&[]),
        Err(DeclarationError::IndexedFieldNotScalar("avatar".into()))
    );
}

#[test]
fn remote_without_operation_is_a_fault() {
    let nameless = RemoteSpec { operation: String::new(), ..complete_remote() };
    assert_eq!(nameless.fault(), Some(RemoteFault::MissingOperation));
    assert_eq!(complete_remote().fault(), None);
}

#[test]
fn action_refetch_must_name_a_remote_relation_and_optimistic_a_field() {
    let mut remote_relation = RelationSpec::global("feed");
    remote_relation.remote = Some(complete_remote());
    let host = |refetch: &str, optimistic: &str| {
        let mut declared = action("like", RootShape::Upsert);
        declared.refetch.push(refetch.into());
        declared.optimistic.insert(optimistic.into(), serde_json::json!(true));
        ModelSpec { actions: vec![declared], relations: vec![remote_relation.clone()], ..model(vec![id_field()]) }
    };
    let mut ok = host("feed", "id");
    assert_eq!(ok.validate(&[]), Ok(()));
    ok = host("missing", "id");
    assert_eq!(ok.validate(&[]), Err(DeclarationError::UnknownRefetch { action: "like".into(), relation: "missing".into() }));
    ok = host("feed", "ghost");
    assert_eq!(ok.validate(&[]), Err(DeclarationError::UnknownOptimisticField { action: "like".into(), field: "ghost".into() }));
}

#[test]
fn field_with_a_path_reads_the_nested_value() {
    let field = FieldSpec::scalar("locationLat", ScalarKind::Num).read_from("location.lat");

    assert_eq!(field.read(&object(json!({"location": {"lat": 1.5}}))), Some(json!(1.5)));
    assert_eq!(field.read(&object(json!({"locationLat": 2.5, "location": {"lat": 1.5}}))), Some(json!(2.5)));
    assert_eq!(field.read(&object(json!({"location": {"city": "Kyiv"}}))), None);
    assert_eq!(field.read(&object(json!({"location": null}))), None);
}

#[test]
fn the_id_counts_as_an_indexed_column() {
    let spec = ModelSpec::new("m", vec![FieldSpec::raw("id")]);
    assert!(spec.is_indexed("id"));
    assert!(!spec.is_indexed("other"));
}

#[test]
fn local_rows_get_zero_values_for_fields_the_server_owns() {
    let spec = ModelSpec::new(
        "chats",
        vec![
            id_field(),
            FieldSpec::scalar("name", ScalarKind::Str),
            FieldSpec::scalar("unread", ScalarKind::Int),
            FieldSpec::scalar("muted", ScalarKind::Bool),
            FieldSpec::scalar("createdAt", ScalarKind::Date),
            FieldSpec::scalar("ownerId", ScalarKind::Id).nullable(),
        ],
    );
    let row = spec.normalize_local(&json!({"id": "c1", "name": "Ann"}).as_object().unwrap().clone(), 1_000).unwrap();
    assert_eq!(row["name"], json!("Ann"));
    assert_eq!((row["unread"].clone(), row["muted"].clone()), (json!(0), json!(false)));
    assert_eq!(row["createdAt"], json!("1970-01-01T00:00:01.000Z"));
    assert!(spec.normalize(&json!({"id": "c1", "name": "Ann"}).as_object().unwrap().clone()).is_err());
}
