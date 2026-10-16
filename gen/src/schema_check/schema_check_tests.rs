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
input ItemInput { title: String! note: String file: String! count: Int momentId: Int ownerId: ID withDefault: String! = "x" ids: [ID!] }
type Mutation {
  createItem(input: ItemInput!): Item
  tagItem(itemId: ID!, label: String, size: Int!): Item
}
"#;

const OPERATIONS: &str = "query items($first: Int) { items(first: $first) { nodes { id } } }\nquery mine { mine: items { nodes { id } } }\nquery one($id: ID!) { item(id: $id) { id } }\nquery all { plain { id } }\nmutation CreateItem($input: ItemInput!) { createItem(input: $input) { id } }\nmutation TagItem($itemId: ID!, $label: String, $size: Int!) { tagItem(itemId: $itemId, label: $label, size: $size) { id } }\n";

const REMOTE: &str = "  scope :all do\n    fetch \"items\"\n    paginate 20\n  end\n";

fn model(fields: &str, remote: &str) -> String {
    format!("model Item do\n\n{fields}\n\n{remote}end\n")
}

fn run(source: &str) -> Result<Vec<String>, Vec<(usize, String)>> {
    let directory = tempfile::tempdir().unwrap();
    let schema = directory.path().join("schema.graphql");
    fs::write(&schema, SCHEMA).unwrap();
    fs::create_dir(directory.path().join("queries")).unwrap();
    fs::write(directory.path().join("queries/items.graphql"), OPERATIONS).unwrap();
    let file = directory.path().join("item.dbl");
    fs::write(&file, source).unwrap();
    let scalars = directory.path().join("scalars.dbl");
    fs::write(&scalars, "scalar :DateTime, as: :date\n").unwrap();
    let operations: Vec<PathBuf> = vec![directory.path().join("queries")];
    check_models_against_schema(&schema, &operations, &[scalars, file]).map_err(|errors| errors.into_iter().map(|error| (error.error.line, error.error.message)).collect())
}

#[test]
fn declarations_are_checked_against_the_schema() {
    let valid = "    field :title, type: :string\n    field :note, type: :string, null: true\n    field :count, type: :int\n    field :tags, type: [:string]\n    field :kind, type: :enum, values: [:A, :B]\n    field :createdAt, type: :date\n    field :ownerName, type: :string, null: true, from: \"owner.name\"\n    field :meta, type: MetaBox, null: true do\n      field :label, type: :string\n      field :inner, type: InnerBox, null: true do\n        field :x, type: :int\n      end\n    end\n    field :ghost, type: :string, local: true, null: true\n";
    let cases: [(&str, String, Option<(usize, &str)>); 19] = [
        ("every kind fits", model(valid, REMOTE), None),
        ("missing field", model("    field :ghost, type: :string\n", REMOTE), Some((3, "type `Item` has no field `ghost`"))),
        ("nullable in schema, required in model", model("    field :note, type: :string\n", REMOTE), Some((3, "declare `null: true`"))),
        ("list declared as scalar", model("    field :tags, type: :string\n", REMOTE), Some((3, "which is a list"))),
        ("scalar declared as list", model("    field :title, type: [:string]\n", REMOTE), Some((3, "not a list"))),
        ("wrong scalar kind", model("    field :title, type: :bool\n", REMOTE), Some((3, "does not fit the schema type"))),
        ("enum value the schema lacks", model("    field :kind, type: :enum, values: [:A, :Z]\n", REMOTE), Some((3, "enum `Kind` does not have"))),
        ("from path missing", model("    field :ownerName, type: :string, null: true, from: \"owner.ghost\"\n", REMOTE), Some((3, "type `Owner` has no field `ghost`"))),
        ("nested field missing", model("    field :meta, type: MetaBox, null: true do\n      field :gone, type: :string\n    end\n", REMOTE), Some((4, "type `Meta` has no field `gone`"))),
        ("unknown root field", model("    field :title, type: :string\n", "  scope :all do\n    fetch \"nothing\"\n    paginate 20\n  end\n"), Some((7, "operation `nothing` is not in the operation documents"))),
        ("single remote on a list", model("    field :title, type: :string\n", "  scope :one do\n    fetch \"all\"\n  end\n"), Some((7, "returns `[Item!]!`"))),
        ("list remote on one object", model("    field :title, type: :string\n", "  scope :one do\n    fetch \"one\"\n    list\n  end\n"), Some((8, "expects a list"))),
        ("aliased root needs its key", model("    field :title, type: :string\n", "  scope :mine do\n    fetch \"mine\", root: \"items\"\n    paginate 20\n  end\n"), Some((7, "must start with the first field `mine`"))),
        ("aliased root with its key", model("    field :title, type: :string\n", "  scope :mine do\n    fetch \"mine\"\n    paginate 20\n  end\n"), None),
        ("id from the parent object", "model Item do\n\n    field :id, type: :id, id_from: :owner\n    field :ownerId, type: :id, null: true\n\n\n  belongs_to :owner, model: Item, from: \"owner\"\n  scope :one do\n    fetch \"one\"\n  end\nend\n".to_string(), None),
        ("association path of the wrong shape", "model Item do\n\n    field :title, type: :string\n    field :ownerId, type: :id, null: true\n\n\n  has_many :owners, model: Item, key: :ownerId, from: \"owner\"\n  scope :one do\n    fetch \"one\"\n  end\nend\n".to_string(), Some((7, "expects a list"))),
        ("default covers a nullable schema field", model("    field :note, type: :string, default: \"\"\n", REMOTE), None),
        ("association path through a list", "model Item do\n\n    field :title, type: :string\n    field :ownerIds, type: [:id], default: []\n\n\n  references :owners, model: Item, key: :ownerIds, from: \"links.owner\"\n  scope :one do\n    fetch \"one\"\n  end\nend\n".to_string(), None),
        ("local model has no schema", "model Item, storage: :local do\n\n    field :ghost, type: :string\n\nend\n".to_string(), None),
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
    let notes = run(&model("    field :title, type: :string\n", REMOTE)).unwrap();
    assert!(notes.iter().any(|note| note.contains("model `Item` scope `all` does not use") && note.contains("extra")), "{notes:?}");
}

fn with_lines(lines: &str) -> String {
    model("    field :title, type: :string\n", lines)
}

fn action_with(lines: &str) -> String {
    with_lines(&format!("  operation :tag do\n    mutation TagItem\n    local_effect :none, reason: \"test\"\n{lines}\n  end\n"))
}

#[test]
fn args_are_checked_against_the_input_of_their_mutation() {
    let create = |wrap: &str, input: &str, args: &str| with_lines(&format!("  operation :create do\n    mutation CreateItem\n    {wrap}\n    input {input}\n    args {args}\n  end\n"));
    let wrapped = |input: &str, args: &str| create("wrap :input", input, args);
    let cases: [(&str, String, Option<(usize, &str)>); 15] = [
        ("string for String!", wrapped("title: :title, file: :file", "file: :string"), None),
        ("nullable arg for a nullable input", wrapped("title: :title, note: :note", "note: :string?"), None),
        ("a required arg may fill a nullable input", wrapped("title: :title, note: :note, file: :file", "note: :string, file: :string"), None),
        ("id arg for an Int input", wrapped("title: :title, momentId: :momentId, file: :f", "momentId: :id?, f: :string"), None),
        ("id arg for an ID input", wrapped("title: :title, ownerId: :ownerId, file: :f", "ownerId: :id?, f: :string"), None),
        ("ids arg for [ID!]", wrapped("title: :title, ids: :ids, file: :f", "ids: :ids?, f: :string"), None),
        ("input with a default is optional", wrapped("title: :title, withDefault: :withDefault, file: :f", "withDefault: :string?, f: :string"), None),
        ("variable the input type lacks", wrapped("title: :title, ghost: :ghost", "ghost: :string"), Some((6, "`ghost` is not an input of `ItemInput`"))),
        ("kind that does not fit", wrapped("title: :title, file: :file", "file: :bool"), Some((6, "arg `file` of kind `:bool` does not fit `file: String!`"))),
        ("string for an Int input", wrapped("title: :title, count: :count, file: :f", "count: :string?, f: :string"), Some((6, "does not fit `count: Int`"))),
        ("nullable arg for a required input", wrapped("title: :title, file: :file", "file: :string?"), Some((6, "arg `file` may be nil but `file: String!` is required"))),
        ("operation that is not in the documents", with_lines("  operation :create do\n    mutation Nope\n    input a: :a\n    args a: :string\n  end\n"), Some((6, "operation `Nope` is not in the operation documents"))),
        ("unwrapped action takes the variables", action_with("    args label: :string?, size: :int\n    input itemId: :id, label: :label, size: :size"), None),
        ("unwrapped action with a wrong kind", action_with("    args size: :string\n    input itemId: :id, size: :size"), Some((6, "arg `size` of kind `:string` does not fit `size: Int!`"))),
        ("wrap that is not a variable", with_lines("  operation :create do\n    mutation CreateItem\n    wrap :payload\n    input title: :title, file: :file\n    args file: :string\n  end\n"), Some((6, "operation `CreateItem` has no variable `$payload`"))),
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
