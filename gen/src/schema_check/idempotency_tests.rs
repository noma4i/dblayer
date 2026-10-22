use super::*;

#[test]
fn idempotency_is_inferred_from_the_mutation_input_without_model_fields() {
    let directory = tempfile::tempdir().unwrap();
    let schema = directory.path().join("schema.graphql");
    let document = directory.path().join("create.graphql");
    let model = directory.path().join("item.dbl");
    fs::write(&document, "mutation CreateItem($input: ItemInput!) { createItem(input: $input) { id title } }").unwrap();
    fs::write(&model, "model Item do\n  attribute :title, :string\n  operation :create do\n    mutation \"CreateItem\"\n    wrap \"input\"\n    variables title: :title\n  end\nend\n").unwrap();
    for (field, expected) in [("clientMutationId: String", true), ("", false)] {
        fs::write(&schema, format!("type Query {{ item: Item }} type Item {{ id: ID! title: String! }} input ItemInput {{ title: String! {field} }} type Mutation {{ createItem(input: ItemInput!): Item }}")).unwrap();
        let declared = crate::compile_files(&[&model], Some(&schema), std::slice::from_ref(&document)).unwrap();
        let action = serde_json::to_value(&declared.models[0].actions[0]).unwrap();
        assert_eq!(action["client_mutation_id"], serde_json::json!(expected));
        assert!(declared.models[0].fields.iter().all(|field| field.name != "clientMutationId"));
    }
}
