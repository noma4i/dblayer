use super::schema_tests::*;
use super::*;
use serde_json::json;

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
fn every_reserved_member_is_refused_as_a_relation_and_an_association_name() {
    let names = reserved_member_names();
    assert_eq!(names.iter().collect::<HashSet<_>>().len(), names.len());
    for name in &names {
        let relation = relation_host(RelationSpec::new(name, &["chatId"])).validate(&names);
        let association = ModelSpec { associations: vec![association(name, AssociationKind::References, "chats", "")], ..relation_host(RelationSpec::new("thread", &["chatId"])) }.validate(&names);
        let wanted = if *name == "sync" { DeclarationError::ReservedRelationName(name.to_string()) } else { DeclarationError::NameCollision(name.to_string()) };
        assert_eq!(relation, Err(wanted), "relation {name}");
        assert_eq!(association, Err(DeclarationError::NameCollision(name.to_string())), "association {name}");
    }
    assert!(generated_member_names().iter().all(|name| names.contains(name)));
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
fn typed_params_must_be_int_or_bool_text_params() {
    let typed = |kind: ScalarKind, listed: bool| RelationSpec {
        text_params: if listed { vec!["after".into()] } else { Vec::new() },
        scalar_types: [("after".to_string(), kind)].into(),
        ..RelationSpec::new("since", &["chatId"])
    };
    assert_eq!(relation_host(typed(ScalarKind::Int, true)).validate(&[]), Ok(()));
    assert_eq!(relation_host(typed(ScalarKind::Bool, true)).validate(&[]), Ok(()));
    let refused = Err(DeclarationError::ScalarParamKind { relation: "since".into(), param: "after".into() });
    assert_eq!(relation_host(typed(ScalarKind::Date, true)).validate(&[]), refused);
    assert_eq!(relation_host(typed(ScalarKind::Int, false)).validate(&[]), refused);
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

pub(in super::super) fn action(name: &str, root_shape: RootShape) -> ActionDeclaration {
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

    assert_eq!(with_actions(vec![with_member("after")]).validate(&[]), Err(DeclarationError::ForbiddenMember("after".into())));
    assert_eq!(with_actions(vec![with_member("resume")]).validate(&[]), Err(DeclarationError::ForbiddenMember("resume".into())));
    assert_eq!(with_actions(vec![with_member("commit")]).validate(&[]), Ok(()));
}

#[test]
fn specs_round_trip_through_json_and_default_missing_parts() {
    let spec = ModelSpec {
        associations: vec![association("chat", AssociationKind::BelongsTo, "chats", "chatId")],
        relations: vec![RelationSpec { remote: Some(paged_remote(true)), ..RelationSpec::new("thread", &["chatId"]) }],
        indexes: vec![vec!["chatId".into(), "sequenceNumber".into()]],
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

    let remote = RelationSpec { updates: vec![UpdatesSpec { apply: None, subscription: "MomentEvents".into(), deleted_by: Some("destroyed".into()), deleted_unless: None, set: Map::new(), debounce: None, root: None, id: Vec::new() }], ..RelationSpec::global("feed") };
    let text = serde_json::to_string(&remote).unwrap();
    assert_eq!(serde_json::from_str::<RelationSpec>(&text).unwrap(), remote);
    let bare = r#"{"operation":"Q","shape":{"type":"single"},"stale_time":"5.minutes"}"#;
    assert_eq!(serde_json::from_str::<RemoteSpec>(bare).unwrap().operation, "Q");

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

    for (input, expected) in [
        (json!({"location": {"lat": 1.5}}), Some(json!(1.5))),
        (json!({"locationLat": 2.5, "location": {"lat": 1.5}}), Some(json!(2.5))),
        (json!({"location": {"town": "Harbor"}}), None),
        (json!({}), None),
        (json!({"location": null}), Some(Value::Null)),
        (json!({"location": {"lat": null}}), Some(Value::Null)),
    ] {
        assert_eq!(field.read(&object(input.clone())), expected, "{input}");
    }
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
    assert!(matches!(spec.normalize(&json!({"id": "c1", "name": "Ann"}).as_object().unwrap().clone()), Err(crate::schema::SchemaError::MissingField(_))));
}

#[test]
fn a_null_from_the_server_never_empties_a_field_that_has_a_default() {
    let spec = model(vec![
        FieldSpec::scalar("id", ScalarKind::Str),
        FieldSpec::array("location").with_default(json!([])),
        FieldSpec::scalar("body", ScalarKind::Str).nullable(),
    ]);
    let stored = object(json!({"id": "1", "location": [{"city": "Oslo"}], "body": "hi"}));

    let merged = spec.merged(Some(&stored), &spec.patch(&object(json!({"id": "1", "location": null, "body": null})))).unwrap();

    assert_eq!(merged, object(json!({"id": "1", "location": [], "body": null})));
}

#[test]
fn a_set_or_compared_field_must_be_declared_on_its_side() {
    let chats = ModelSpec::new("chats", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("lastAt", ScalarKind::Date).nullable().optional().derived()]);
    let fields = vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("chatId", ScalarKind::Id).indexed(), FieldSpec::scalar("createdAt", ScalarKind::Date).optional()];
    let faults = |parent_field: &str, child_field: &str, compared: &str| {
        let mut chat = association("chat", AssociationKind::BelongsTo, "chats", "chatId");
        chat.sets = [(parent_field.to_string(), child_field.to_string())].into();
        chat.newer = vec![compared.to_string()];
        let child = ModelSpec { associations: vec![chat], ..model(fields.clone()) };
        validate_models(&[chats.clone(), child], &[]).into_iter().map(|fault| fault.error).collect::<Vec<_>>()
    };

    assert_eq!(faults("lastAt", "createdAt", "createdAt"), vec![]);
    let unknown = vec![DeclarationError::SetsUnknownField("chat".into())];
    assert_eq!(faults("lastAt", "missing", "createdAt"), unknown);
    assert_eq!(faults("nope", "createdAt", "createdAt"), unknown);
    assert_eq!(faults("lastAt", "createdAt", "nope"), unknown);
}

#[test]
fn a_merge_rule_must_fit_the_type_of_its_field() {
    use crate::schema::MergeRule;
    let faults = |field: FieldSpec, rule: MergeRule| {
        let mut field = field;
        field.merge = Some(rule);
        let spec = ModelSpec::new("things", vec![FieldSpec::scalar("id", ScalarKind::Id), field]);
        validate_models(&[spec], &[]).into_iter().map(|fault| fault.error).collect::<Vec<_>>()
    };
    let ladder = |names: &[&str]| MergeRule::Ladder { tiers: vec![names.iter().map(|name| name.to_string()).collect()] };
    assert_eq!(faults(FieldSpec::scalar("size", ScalarKind::Int), MergeRule::Positive), vec![]);
    assert_eq!(faults(FieldSpec::object("media"), MergeRule::Keys { keys: Default::default() }), vec![]);
    assert_eq!(faults(FieldSpec::enumeration("state", &["a", "b"]), ladder(&["a"])), vec![]);
    let mismatch = vec![DeclarationError::MergeRuleMismatch("x".into())];
    assert_eq!(faults(FieldSpec::scalar("x", ScalarKind::Str), MergeRule::Positive), mismatch);
    assert_eq!(faults(FieldSpec::scalar("x", ScalarKind::Int), MergeRule::Keys { keys: Default::default() }), mismatch);
    assert_eq!(faults(FieldSpec::enumeration("x", &["a", "b"]), ladder(&["c"])), mismatch);
}

#[test]
fn a_correlation_path_must_start_at_a_declared_field() {
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.mutation = Some("Create".to_string());
    create.has_optimistic = true;
    create.has_temp_row_retention = true;
    let faults = |paths: &[&str]| {
        let mut action = create.clone();
        action.correlate = paths.iter().map(|path| path.to_string()).collect();
        let spec = ModelSpec { actions: vec![action], ..ModelSpec::new("things", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::object("media").nullable()]) };
        validate_models(&[spec], &[]).into_iter().map(|fault| fault.error).collect::<Vec<_>>()
    };
    assert_eq!(faults(&["id", "media.blobId"]), vec![]);
    assert_eq!(faults(&["media.blobId", "missing"]), vec![DeclarationError::CorrelateUnknownField("create".into())]);
}

#[test]
fn inv_dsl_10_not_values_pass_only_a_present_string_that_is_not_listed() {
    let except: MatchValues = serde_json::from_value(serde_json::json!({"not": ["SCREENSHOT"]})).unwrap();
    assert!(except.accepts(&serde_json::json!("TYPING")));
    for refused in [serde_json::json!("SCREENSHOT"), serde_json::Value::Null, serde_json::json!(1), serde_json::json!(["TYPING"])] {
        assert!(!except.accepts(&refused), "{refused} passed a not filter");
    }
    let one_of: MatchValues = serde_json::from_value(serde_json::json!(["A", "B"])).unwrap();
    assert!(one_of.accepts(&serde_json::json!("A")) && !one_of.accepts(&serde_json::json!("C")) && !one_of.accepts(&serde_json::Value::Null));
}

#[test]
fn inv_dsl_11_membership_binds_a_list_parameter_to_a_row_field_by_contains_or_equals() {
    let mut contains = RelationSpec::new("list", &[]);
    contains.membership.insert("list".into(), Membership::Contains("lists".into()));
    let row = serde_json::json!({"id": "c1", "lists": ["PRIMARY", "PINNED"]}).as_object().unwrap().clone();
    let params: Vec<_> = contains.row_params(&row).iter().map(|params| params["list"].clone()).collect();
    assert_eq!(params, [serde_json::json!("PRIMARY"), serde_json::json!("PINNED")]);
    assert!(contains.row_params(&serde_json::json!({"id": "c1"}).as_object().unwrap().clone()).is_empty(), "a row without the field joins nothing");
    let mut equals = RelationSpec::new("by_title", &[]);
    equals.membership.insert("title".into(), Membership::Equals("title".into()));
    assert_eq!(equals.row_params(&serde_json::json!({"id": "c1", "title": "t"}).as_object().unwrap().clone()).len(), 1);
    assert!(equals.row_params(&serde_json::json!({"id": "c1", "title": null}).as_object().unwrap().clone()).is_empty());
}
