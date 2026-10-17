use dblayer::db::Db;
use serde_json::json;
use super::*;

#[test]
fn keyed_reactions_use_the_declared_field_with_a_nested_source() {
    let source = r#"model Message do
  field :reactions, type: [MessageReactionNode], default: [] do
    field :body, type: :string, null: true
    field :userId, type: :id, null: true, from: "user.id"
    field :user, type: MessageUserRef, null: true do
      field :id, type: :id, null: true
    end
  end
  operation :react do
    mutation MessageReact
    args reactionBody: :string, userId: :id
    optimistic reactions: { by: :userId, element: { userId: :userId, body: :reactionBody } }
  end
end"#;
    let models = parse_models(source).unwrap();
    let patch = models[0].actions[0].optimistic.clone();
    let db = Db::open_in_memory(models, None).unwrap();
    let id = dblayer::identity::RowId::new("m1").unwrap();
    let scope = dblayer::commit::ScopeRef::new(dblayer::identity::ModelKey::new("Message"), dblayer::identity::ScopeKey::from_raw("all"));
    let request = dblayer::db::ReplaceRequest::new(scope, dblayer::schema::Coverage::Complete);
    db.replace(&request, vec![json!({"id": "m1", "reactions": [{"user": {"id": "u1"}, "body": "A"}, {"user": {"id": "u2"}, "body": "B"}]}).as_object().unwrap().clone()]).unwrap();
    for (body, expected) in [("C", vec!["C", "B"]), ("", vec!["B"])] {
        db.act("Message", "react", &id, patch.clone(), json!({"userId": "u1", "reactionBody": body}).as_object().unwrap().clone()).unwrap();
        let found = db.find("Message", &id).unwrap().unwrap();
        let reactions = found.data["reactions"].as_array().unwrap();
        assert_eq!(reactions.iter().map(|reaction| reaction["body"].as_str().unwrap()).collect::<Vec<_>>(), expected);
    }
}

#[test]
fn nested_types_keep_optional_fields_and_work_in_local_models() {
    let models = crate::parse_sources(&[include_str!("../../fixtures/nested_reference.dbl"), include_str!("../../fixtures/nested.dbl")]).unwrap();
    for (source, read) in [(emit_swift(&models, &[], &SchemaTypes::default()), "note = try container.decodeIfPresent(String.self, forKey: .note)"), (emit_kotlin(&models, &[], &SchemaTypes::default()), "val note: String? = null")] {
        assert!(source.contains(read), "optional note was lost: {read}");
    }
    let db = Db::open_in_memory(models, None).unwrap();
    let value = json!({"title": "local", "note": "kept", "child": {"body": "nested"}});
    let row = db.create("NestedSample", json!({"value": value, "values": []}).as_object().unwrap().clone()).unwrap();
    assert_eq!(db.find("NestedSample", &row.id).unwrap().unwrap().data["value"], value);
}

#[test]
fn nested_declarations_reject_invalid_fields_names_references_and_defaults_at_their_line() {
    for (fields, error) in [
        ("field :v, type: Missing", "not declared"),
        ("field :v, type: :object do\nend", "at least 1 field"),
        ("field :v, type: :object do\nfield :a, type: :string\nfield :a, type: :string\nend", "declared twice"),
        ("field :v, type: Sample do\nfield :a, type: :string\nend", "name of a model"),
        ("field :v, type: :object do\nfield :a, type: :string, default: 3\nend", "default must"),
        ("field :v, type: :object do\nfield :a, type: :num, from: \"\"\nend", "non-empty"),
        ("field :v, type: :object do\nfield :a, type: :num, from: \"user..id\"\nend", "empty segment"),
        ("field :v, type: :object do\nfield :a, type: :enum, values: [:yes], default: :no\nend", "default must"),
        ("field :v, type: :object do\nfield :a, type: [:bool], default: [1]\nend", "default must"),
        ("field :v, type: :object do\nfield :child, type: :object, default: {} do\nfield :required, type: :string\nend\nend", "default must"),
        ("field :v, type: Recursive do\nfield :child, type: Recursive, null: true\nend", "recursive objects"),
        ("field :v, type: :object do\nfield :a, type: :file\nend", "unknown word"),
        ("field :v, type: :object do\nfield :a, type: :string, index: true\nend", "declare indexes"),
        ("field :v, type: [\"[Value]\"]", "type name"),
        ("field :v, type: :raw, as: Value", "declare named objects and lists"),
        ("field :v, type: Value do\nfield :a, type: :string\nend\nfield :w, type: Value do\nfield :a, type: :string\nend", "declared twice"),
    ] {
        let source = format!("model Sample do\n\n{fields}\n\nend");
        let errors = parse_models(&source).expect_err(&source);
        assert!(errors.iter().any(|found| found.line >= 3 && found.message.contains(error)), "{source}: {errors:?}");
    }
}
