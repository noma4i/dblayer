use std::fs;

use crate::check_models_against_schema;

const SCHEMA: &str = r#"
type Query { node(id: ID!): Node things(owner: String, tag: ID, limit: Int): [Thing!]! }
interface Node { id: ID! }
type Thing implements Node { id: ID! title: String! }
"#;

const MODEL: &str = "model Thing do\n  attribute :title, :string\n  scope :one, node: :one do\n    parameter :id, :id\n    query \"ThingNode\"\n    variables id: :id\n  end\n  scope :owned do\n    parameter :owner, :string\n    query \"ThingsOwned\"\n    response from: \"things\", shape: :list\n  end\nend\n";

const NODE: &str = "query ThingNode($id: ID!) { node(id: $id) { ... on Thing { id title } } }\n";
const OWNED: &str = "query ThingsOwned($owner: String) { things(owner: $owner) { id title } }\n";

fn run(model: &str, operations: &str) -> Result<(), Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let schema = directory.path().join("schema.graphql");
    fs::write(&schema, SCHEMA).unwrap();
    fs::create_dir(directory.path().join("queries")).unwrap();
    fs::write(directory.path().join("queries/all.graphql"), operations).unwrap();
    let file = directory.path().join("thing.dbl");
    fs::write(&file, model).unwrap();
    check_models_against_schema(&schema, &[directory.path().join("queries")], &[file]).map(|_| ()).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

#[test]
fn every_scope_variable_names_a_document_variable_and_every_required_one_has_a_source() {
    let documents = |node: &str, owned: &str| format!("{node}{owned}");
    let cases: [(&str, String, String, &[&str]); 5] = [
        ("names match", MODEL.to_string(), documents(NODE, OWNED), &[]),
        ("a renamed node id", MODEL.replace("variables id: :id", "variables thingId: :id"), documents("query ThingNode($thingId: ID!) { node(id: $thingId) { ... on Thing { id title } } }\n", OWNED), &[]),
        ("a node scope binds `id` while the document asks for `$thingId`", MODEL.to_string(), documents("query ThingNode($thingId: ID!) { node(id: $thingId) { ... on Thing { id title } } }\n", OWNED), &["model `Thing` scope `one` operation `ThingNode`: variable `$id` is not a variable of the document", "model `Thing` scope `one` operation `ThingNode`: variable `$thingId` has no source"]),
        ("a string parameter the document does not declare", MODEL.to_string(), documents(NODE, "query ThingsOwned($ownerName: String) { things(owner: $ownerName) { id title } }\n"), &["scope `owned` param `owner` is `string` but operation `ThingsOwned` has no variable `$owner`"]),
        ("a required document variable without a source", MODEL.to_string(), documents(NODE, "query ThingsOwned($owner: String, $tag: ID!) { things(owner: $owner, tag: $tag) { id title } }\n"), &["model `Thing` scope `owned` operation `ThingsOwned`: variable `$tag` has no source"]),
    ];
    for (name, model, operations, expected) in cases {
        match (run(&model, &operations), expected.is_empty()) {
            (Ok(()), true) => {}
            (Ok(()), false) => panic!("{name}: accepted"),
            (Err(errors), true) => panic!("{name}: {errors:?}"),
            (Err(errors), false) => {
                for message in expected {
                    assert!(errors.iter().any(|text| text.contains(message)), "{name}: {message} missing in {errors:?}");
                }
            }
        }
    }
}
