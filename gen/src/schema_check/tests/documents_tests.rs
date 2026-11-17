use std::fs;
use std::path::PathBuf;

use crate::check_models_against_schema;

const SCHEMA: &str = "type Query { items: ItemList!, one(id: ID!): Item }\ntype Mutation { touch(id: ID!): Item }\ntype ItemList { nodes: [Item!]! }\ntype Item { id: ID! title: String! owner: Owner }\ntype Owner { id: ID! name: String }\n";

const MODEL: &str = "model Item do\n  attribute :title, :string\n  scope :one do\n    parameter :itemId, :id\n    variables id: :itemId\n    query \"one\"\n  end\nend\n";

fn check(documents: &str) -> Result<(), Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let schema = directory.path().join("schema.graphql");
    fs::write(&schema, SCHEMA).unwrap();
    fs::create_dir(directory.path().join("queries")).unwrap();
    fs::write(directory.path().join("queries/all.graphql"), documents).unwrap();
    let model = directory.path().join("item.dbl");
    fs::write(&model, MODEL).unwrap();
    let operations: Vec<PathBuf> = vec![directory.path().join("queries")];
    check_models_against_schema(&schema, &operations, &[model]).map(drop).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

#[test]
fn gen_checks_documents_against_the_schema() {
    let valid = "query one($id: ID!) { one(id: $id) { id title owner { ...OwnerFields } } }\nfragment OwnerFields on Owner { id name }\n";
    let cases: [(&str, String, Option<&str>); 7] = [
        ("every selection exists", valid.to_string(), None),
        ("a field the type lacks", valid.replace("title", "caption"), Some("type `Item` has no field `caption`")),
        ("a root field the schema lacks", valid.replace("one(id: $id)", "uno(id: $id)"), Some("type `Query` has no field `uno`")),
        ("a field inside a fragment the type lacks", valid.replace("id name", "id nickname"), Some("type `Owner` has no field `nickname`")),
        ("a variable of a type the schema lacks", valid.replace("$id: ID!", "$id: Identifier!"), Some("type `Identifier` which the schema does not have")),
        ("a fragment on a type the schema lacks", valid.replace("on Owner", "on Person"), Some("type `Person` which the schema does not have")),
        ("a selection below a scalar", valid.replace("title", "title { size }"), Some("type `String` has no fields")),
    ];
    for (name, documents, expected) in cases {
        match (check(&documents), expected) {
            (Ok(()), None) => {}
            (Ok(()), Some(_)) => panic!("{name}: accepted"),
            (Err(errors), None) => panic!("{name}: {errors:?}"),
            (Err(errors), Some(message)) => assert!(errors.iter().any(|text| text.contains(message)), "{name}: {errors:?}"),
        }
    }
}
