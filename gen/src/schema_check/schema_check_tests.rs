use std::fs;
use std::path::PathBuf;

use crate::check_models_against_schema;

const SCHEMA: &str = r#"
scalar DateTime
enum Kind { A B }
type Query {
  items(first: Int): ItemConnection!
  item(id: ID!): Item
  plain: [Item!]!
}
type ItemConnection { nodes: [Item!]! pageInfo: PageInfo! }
type PageInfo { hasNextPage: Boolean! }
type Item {
  id: ID!
  title: String!
  note: String
  count: Int!
  tags: [String!]!
  kind: Kind!
  createdAt: DateTime!
  owner: Owner
  meta: Meta
  extra: String
  links: [Link!]!
}
type Link { owner: Owner }
type Owner { id: ID! name: String }
type Meta { label: String! inner: Inner }
type Inner { x: Int! }
"#;

const OPERATIONS: &str = "query items($first: Int) { items(first: $first) { nodes { id } } }\nquery mine { mine: items { nodes { id } } }\nquery one($id: ID!) { item(id: $id) { id } }\nquery all { plain { id } }\n";

const REMOTE: &str = "  remote :all do\n    query \"items\"\n    paginate 20\n  end\n";

fn model(fields: &str, remote: &str) -> String {
    format!("model Item do\n  schema do\n{fields}  end\n\n{remote}end\n")
}

fn run(source: &str) -> Result<Vec<String>, Vec<(usize, String)>> {
    let directory = tempfile::tempdir().unwrap();
    let schema = directory.path().join("schema.graphql");
    fs::write(&schema, SCHEMA).unwrap();
    fs::create_dir(directory.path().join("queries")).unwrap();
    fs::write(directory.path().join("queries/items.graphql"), OPERATIONS).unwrap();
    let file = directory.path().join("item.dbl");
    fs::write(&file, source).unwrap();
    let operations: Vec<PathBuf> = vec![directory.path().join("queries")];
    check_models_against_schema(&schema, &operations, &[file]).map_err(|errors| errors.into_iter().map(|error| (error.error.line, error.error.message)).collect())
}

#[test]
fn declarations_are_checked_against_the_schema() {
    let valid = "    string :title\n    string :note, null: true\n    int :count\n    list :tags, of: :string\n    enum :kind, values: [:A, :B]\n    date :createdAt\n    string :ownerName, null: true, from: \"owner.name\"\n    object :meta, as: MetaBox, null: true do\n      string :label\n      object :inner, as: InnerBox, null: true do\n        int :x\n      end\n    end\n    string :ghost, local: true, null: true\n";
    let cases: [(&str, String, Option<(usize, &str)>); 19] = [
        ("every kind fits", model(valid, REMOTE), None),
        ("missing field", model("    string :ghost\n", REMOTE), Some((3, "type `Item` has no field `ghost`"))),
        ("nullable in schema, required in model", model("    string :note\n", REMOTE), Some((3, "declare `null: true`"))),
        ("list declared as scalar", model("    string :tags\n", REMOTE), Some((3, "which is a list"))),
        ("scalar declared as list", model("    list :title, of: :string\n", REMOTE), Some((3, "not a list"))),
        ("wrong scalar kind", model("    bool :title\n", REMOTE), Some((3, "does not fit the schema type"))),
        ("enum value the schema lacks", model("    enum :kind, values: [:A, :Z]\n", REMOTE), Some((3, "enum `Kind` does not have"))),
        ("from path missing", model("    string :ownerName, null: true, from: \"owner.ghost\"\n", REMOTE), Some((3, "type `Owner` has no field `ghost`"))),
        ("nested field missing", model("    object :meta, as: MetaBox, null: true do\n      string :gone\n    end\n", REMOTE), Some((4, "type `Meta` has no field `gone`"))),
        ("unknown root field", model("    string :title\n", "  remote :all do\n    query \"nothing\"\n    paginate 20\n  end\n"), Some((7, "operation `nothing` is not in the operation documents"))),
        ("single remote on a list", model("    string :title\n", "  remote :one do\n    query \"all\"\n  end\n"), Some((7, "returns `[Item]!`"))),
        ("list remote on one object", model("    string :title\n", "  remote :one do\n    query \"one\"\n    list\n  end\n"), Some((8, "expects a list"))),
        ("aliased root needs its key", model("    string :title\n", "  remote :mine do\n    query \"mine\", root: \"items\"\n    paginate 20\n  end\n"), Some((7, "must start with the first field `mine`"))),
        ("aliased root with its key", model("    string :title\n", "  remote :mine do\n    query \"mine\"\n    paginate 20\n  end\n"), None),
        ("id from the parent object", "model Item do\n  schema do\n    id :id, id_from: :owner\n    id :ownerId, null: true, index: true\n  end\n\n  belongs_to :owner, model: Item, key: :ownerId, from: \"owner\"\n  remote :one do\n    query \"one\"\n  end\nend\n".to_string(), None),
        ("association path of the wrong shape", "model Item do\n  schema do\n    string :title\n    id :ownerId, null: true, index: true\n  end\n\n  has_many :owners, model: Item, key: :ownerId, from: \"owner\"\n  remote :one do\n    query \"one\"\n  end\nend\n".to_string(), Some((7, "expects a list"))),
        ("default covers a nullable schema field", model("    string :note, default: \"\"\n", REMOTE), None),
        ("association path through a list", "model Item do\n  schema do\n    string :title\n    list :ownerIds, of: :id, default: []\n  end\n\n  references :owners, model: Item, key: :ownerIds, from: \"links.owner\"\n  remote :one do\n    query \"one\"\n  end\nend\n".to_string(), None),
        ("local model has no schema", "model Item, storage: :local do\n  schema do\n    string :ghost\n  end\nend\n".to_string(), None),
    ];
    for (name, source, expected) in cases {
        match (run(&source), expected) {
            (Ok(_), None) => {}
            (Ok(_), Some(_)) => panic!("{name}: accepted"),
            (Err(errors), None) => panic!("{name}: {errors:?}"),
            (Err(errors), Some((line, message))) => assert!(errors.iter().any(|(found, text)| *found == line && text.contains(message)), "{name}: {errors:?}"),
        }
    }
}

#[test]
fn schema_fields_the_model_lacks_are_only_listed() {
    let notes = run(&model("    string :title\n", REMOTE)).unwrap();
    assert!(notes.iter().any(|note| note.contains("model `Item` remote `all` does not use") && note.contains("extra")), "{notes:?}");
}
