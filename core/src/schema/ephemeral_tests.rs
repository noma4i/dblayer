use super::*;
use serde_json::json;

fn host() -> ModelSpec {
    ModelSpec::new(
        "users",
        vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("name", ScalarKind::Str).optional().indexed()],
    )
}

fn typing() -> EphemeralSpec {
    EphemeralSpec::new(FieldSpec::scalar("typing", ScalarKind::Bool).with_default(json!(false))).resetting_after("5.seconds")
}

fn with_ephemeral(spec: EphemeralSpec) -> ModelSpec {
    ModelSpec { ephemeral: vec![spec], ..host() }
}

fn memory() -> ModelSpec {
    ModelSpec { storage: Storage::Memory, ..host() }
}

#[test]
fn a_valid_ephemeral_field_passes_and_stays_out_of_the_stored_row() {
    let model = with_ephemeral(typing());
    assert_eq!(model.validate(&[]), Ok(()));
    let row = model.normalize(json!({"id": "u1", "typing": true}).as_object().unwrap()).unwrap();
    assert!(row.get("typing").is_none());
    assert_eq!(model.ephemeral_field("typing").unwrap().initial(), json!(false));
}

#[test]
fn reset_after_without_a_default_is_rejected() {
    let spec = EphemeralSpec::new(FieldSpec::scalar("typing", ScalarKind::Bool).nullable()).resetting_after("5.seconds");
    assert_eq!(with_ephemeral(spec).validate(&[]), Err(DeclarationError::ResetWithoutDefault("typing".into())));
}

#[test]
fn a_field_without_default_or_null_is_rejected() {
    let spec = EphemeralSpec::new(FieldSpec::scalar("typing", ScalarKind::Bool));
    assert_eq!(with_ephemeral(spec).validate(&[]), Err(DeclarationError::EphemeralNeedsDefault("typing".into())));
    let nullable = EphemeralSpec::new(FieldSpec::scalar("typing", ScalarKind::Bool).nullable());
    assert_eq!(with_ephemeral(nullable).validate(&[]), Ok(()));
}

#[test]
fn an_ephemeral_file_is_rejected() {
    let spec = EphemeralSpec::new(FieldSpec::file("photo").nullable());
    assert_eq!(with_ephemeral(spec).validate(&[]), Err(DeclarationError::EphemeralFile("photo".into())));
}

#[test]
fn an_indexed_ephemeral_field_is_rejected() {
    let spec = EphemeralSpec::new(FieldSpec::scalar("typing", ScalarKind::Bool).with_default(json!(false)).indexed());
    assert_eq!(with_ephemeral(spec).validate(&[]), Err(DeclarationError::EphemeralIndexed("typing".into())));
    let composite = ModelSpec { composite_indexes: vec![vec!["name".into(), "typing".into()]], ..with_ephemeral(typing()) };
    assert_eq!(composite.validate(&[]), Err(DeclarationError::EphemeralIndexed("typing".into())));
}

#[test]
fn an_ephemeral_name_cannot_repeat_a_stored_field() {
    let spec = EphemeralSpec::new(FieldSpec::scalar("name", ScalarKind::Str).nullable());
    assert_eq!(with_ephemeral(spec).validate(&[]), Err(DeclarationError::DuplicateField("name".into())));
    let twice = ModelSpec { ephemeral: vec![typing(), typing()], ..host() };
    assert_eq!(twice.validate(&[]), Err(DeclarationError::DuplicateField("typing".into())));
}

#[test]
fn a_reset_window_must_be_a_positive_duration() {
    assert_eq!(with_ephemeral(typing().resetting_after("soon")).validate(&[]), Err(DeclarationError::InvalidDuration("soon".into())));
    assert_eq!(with_ephemeral(typing().resetting_after("0.seconds")).validate(&[]), Err(DeclarationError::InvalidDuration("0.seconds".into())));
}

#[test]
fn scopes_cannot_filter_or_sort_by_an_ephemeral_field() {
    let by = RelationSpec::new("typists", &["typing"]);
    assert_eq!(
        ModelSpec { relations: vec![by], ..with_ephemeral(typing()) }.validate(&[]),
        Err(DeclarationError::EphemeralInRelation { relation: "typists".into(), field: "typing".into() })
    );
    let mut sorted = RelationSpec::new("by_name", &["name"]);
    sorted.sort = Sort::Fields(vec![SortField { column: "typing".into(), direction: SortDirection::Desc }]);
    assert_eq!(
        ModelSpec { relations: vec![sorted], ..with_ephemeral(typing()) }.validate(&[]),
        Err(DeclarationError::EphemeralInRelation { relation: "by_name".into(), field: "typing".into() })
    );
}

#[test]
fn a_memory_model_has_no_files_and_no_actions() {
    assert_eq!(memory().validate(&[]), Ok(()));
    let with_file = ModelSpec { fields: vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::file("photo").nullable()], ..memory() };
    assert_eq!(with_file.validate(&[]), Err(DeclarationError::MemoryHasFile("photo".into())));
    let with_action = ModelSpec { actions: vec![ActionDeclaration::new("create", RootShape::Upsert)], ..memory() };
    assert_eq!(with_action.validate(&[]), Err(DeclarationError::MemoryHasActions("create".into())));
}

#[test]
fn expire_after_needs_a_memory_model_and_a_positive_duration() {
    let expiring = ModelSpec { expire_after: Some("30.seconds".into()), ..memory() };
    assert_eq!(expiring.validate(&[]), Ok(()));
    assert_eq!(expiring.expire_window(), Some(std::time::Duration::from_secs(30)));
    assert_eq!(ModelSpec { expire_after: Some("30.seconds".into()), ..host() }.validate(&[]), Err(DeclarationError::ExpireNeedsMemory));
    assert_eq!(ModelSpec { expire_after: Some("often".into()), ..memory() }.validate(&[]), Err(DeclarationError::InvalidDuration("often".into())));
}

#[test]
fn updates_set_must_name_ephemeral_fields() {
    let relation = |field: &str| {
        let mut remote = RemoteSpec {
            operation: "UserEvents".into(),
            shape: RemoteShape::Single,
            stale_time: "5.minutes".into(),
            empty_stale_time: None,
            selects_page_info: false,
            updates: vec![UpdatesSpec { subscription: "UserTyping".into(), deleted_by: None, set: Map::new(), removes: false, debounce: None }],
            page_size: None,
            variables: Default::default(), root: None,
        };
        remote.updates[0].set.insert(field.into(), json!(true));
        let mut relation = RelationSpec::global("current");
        relation.remote = Some(remote);
        ModelSpec { relations: vec![relation], ..with_ephemeral(typing()) }
    };
    assert_eq!(relation("typing").validate(&[]), Ok(()));
    let mut unbounded = relation("typing");
    unbounded.ephemeral[0].reset_after = None;
    assert_eq!(unbounded.validate(&[]), Err(DeclarationError::SetWithoutReset { relation: "current".into(), field: "typing".into() }));
    assert_eq!(
        relation("name").validate(&[]),
        Err(DeclarationError::UnknownSetField { relation: "current".into(), field: "name".into() })
    );
}

#[test]
fn old_specs_without_the_new_keys_load_as_disk_models() {
    let text = r#"{"key":"users","fields":[]}"#;
    let model: ModelSpec = serde_json::from_str(text).unwrap();
    assert_eq!((model.storage, model.expire_after, model.ephemeral.len()), (Storage::Disk, None, 0));
    let full = with_ephemeral(typing());
    assert_eq!(serde_json::from_str::<ModelSpec>(&serde_json::to_string(&full).unwrap()).unwrap(), full);
}
