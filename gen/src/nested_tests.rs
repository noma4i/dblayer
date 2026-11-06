use dblayer::db::Db;
use serde_json::json;
use super::*;

#[test]
fn keyed_reactions_use_the_declared_field_with_a_nested_source() {
    let source = r#"model Message do
  attribute :reactions, [MessageReactionNode], default: [] do
    attribute :body, :string, null: true
    attribute :userId, :id, null: true, from: "user.id"
    attribute :user, MessageUserRef, null: true do
      attribute :id, :id, null: true
    end
  end
  operation :react do
    mutation "MessageReact"
    parameter :reactionBody, :string
    parameter :userId, :id
    optimistic reactions: { by: :userId, element: { userId: :userId, body: :reactionBody } }
  end
end"#;
    let models = parse_models(source).unwrap();
    let patch = models[0].actions[0].optimistic.clone();
    let db = Db::open_in_memory(models, None).unwrap();
    let id = dblayer::identity::RowId::new("m1").unwrap();
    let mut answer = dblayer::commit::BatchPlan::from_server(db.generation());
    answer.entity_ops.push(dblayer::commit::EntityOp::Upsert { model: dblayer::identity::ModelKey::new("Message"), id: id.clone(), row: json!({"id": "m1", "reactions": [{"user": {"id": "u1"}, "body": "A"}, {"user": {"id": "u2"}, "body": "B"}]}).as_object().unwrap().clone() });
    db.land(&db.admit().unwrap(), answer).unwrap();
    for (body, expected) in [("C", vec!["C", "B"]), ("", vec!["B"])] {
        db.act(&db.admit().unwrap(), "Message", "react", &id, patch.clone(), json!({"userId": "u1", "reactionBody": body}).as_object().unwrap().clone()).unwrap();
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
    let row = db.create(&db.admit().unwrap(), "NestedSample", json!({"value": value, "values": []}).as_object().unwrap().clone()).unwrap();
    assert_eq!(db.find("NestedSample", &row.id).unwrap().unwrap().data["value"], value);
}

#[test]
fn nested_declarations_reject_invalid_fields_names_references_and_defaults_at_their_line() {
    for (fields, error) in [
        ("attribute :v, Missing", "not declared"),
        ("attribute :v, Value do\nend", "at least 1 attribute"),
        ("attribute :v, Value do\nattribute :a, :string\nattribute :a, :string\nend", "declared twice"),
        ("attribute :v, Sample do\nattribute :a, :string\nend", "name of a model"),
        ("attribute :v, Value do\nattribute :a, :string, default: 3\nend", "default must"),
        ("attribute :v, Value do\nattribute :a, :num, from: \"\"\nend", "empty segment"),
        ("attribute :v, Value do\nattribute :a, :num, from: \"user..id\"\nend", "empty segment"),
        ("attribute :v, Value do\nattribute :a, :enum, values: [\"yes\"], default: \"no\"\nend", "default must"),
        ("attribute :v, Value do\nattribute :a, [:bool], default: [1]\nend", "default must"),
        ("attribute :v, Value do\nattribute :child, Child, default: {} do\nattribute :required, :string\nend\nend", "default must"),
        ("attribute :v, Recursive do\nattribute :child, Recursive, null: true\nend", "recursive objects"),
        ("attribute :v, Value do\nattribute :a, :file\nend", "cannot be a file"),
        ("attribute :v, Value do\nattribute :a, :string, index: true\nend", "unknown key `index`"),
        ("attribute :v, [\"[Value]\"]", "an attribute type is"),
        ("attribute :v, :json, as: Value", "`as:` names the generated enum"),
        ("attribute :v, Value do\nattribute :a, :string\nend\nattribute :w, Value do\nattribute :a, :string\nend", "declared twice"),
    ] {
        let source = format!("model Sample do\n\n{fields}\n\nend");
        let errors = parse_models(&source).expect_err(&source);
        assert!(errors.iter().any(|found| found.line >= 3 && found.message.contains(error)), "{source}: {errors:?}");
    }
}
