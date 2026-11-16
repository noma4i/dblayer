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
  kindItem(itemId: ID!, kind: Kind!): Item
}
"#;

const OPERATIONS: &str = "query items($first: Int) { items(first: $first) { nodes { id } } }\nquery mine { mine: items { nodes { id } } }\nquery one($id: ID!) { item(id: $id) { id } }\nquery all { plain { id } }\nmutation CreateItem($input: ItemInput!) { createItem(input: $input) { id } }\nmutation TagItem($itemId: ID!, $label: String, $size: Int!) { tagItem(itemId: $itemId, label: $label, size: $size) { id } }\nmutation KindItem($itemId: ID!, $kind: Kind!) { kindItem(itemId: $itemId, kind: $kind) { id } }\n";

const REMOTE: &str = "  scope :all do\n    query \"items\"\n    paginate size: 20, direction: :forward\n  end\n";

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

type SchemaCase = (&'static str, String, Option<(usize, &'static str)>);

#[test]
fn declarations_are_checked_against_the_schema() {
    let valid = "    attribute :title, :string\n    attribute :note, :string, null: true\n    attribute :count, :int\n    attribute :tags, [:string]\n    attribute :kind, :enum, values: [\"A\", \"B\"]\n    attribute :createdAt, :date\n    attribute :ownerName, :string, null: true, from: \"owner.name\"\n    attribute :meta, MetaBox, null: true do\n      attribute :label, :string\n      attribute :inner, InnerBox, null: true do\n        attribute :x, :int\n      end\n    end\n    attribute :ghost, :string, local: true, null: true\n";
    let cases: [SchemaCase; 19] = [
        ("every kind fits", model(valid, REMOTE), None),
        ("missing field", model("    attribute :ghost, :string\n", REMOTE), Some((3, "type `Item` has no field `ghost`"))),
        ("nullable in schema, required in model", model("    attribute :note, :string, null: false\n", REMOTE), Some((3, "declare `null: true`"))),
        ("list declared as scalar", model("    attribute :tags, :string\n", REMOTE), Some((3, "which is a list"))),
        ("scalar declared as list", model("    attribute :title, [:string]\n", REMOTE), Some((3, "not a list"))),
        ("wrong scalar kind", model("    attribute :title, :bool\n", REMOTE), Some((3, "does not fit the schema type"))),
        ("enum value the schema lacks", model("    attribute :kind, :enum, values: [\"A\", \"Z\"]\n", REMOTE), Some((3, "enum `Kind` does not have"))),
        ("from path missing", model("    attribute :ownerName, :string, null: true, from: \"owner.ghost\"\n", REMOTE), Some((3, "type `Owner` has no field `ghost`"))),
        ("nested field missing", model("    attribute :meta, MetaBox, null: true do\n      attribute :gone, :string\n    end\n", REMOTE), Some((4, "type `Meta` has no field `gone`"))),
        ("unknown root field", model("    attribute :title, :string\n", "  scope :all do\n    query \"nothing\"\n    paginate size: 20, direction: :forward\n  end\n"), Some((7, "operation `nothing` is not in the operation documents"))),
        ("single remote on a list", model("    attribute :title, :string\n", "  scope :one do\n    query \"all\"\n  end\n"), Some((7, "returns `[Item!]!`"))),
        ("list remote on one object", model("    attribute :title, :string\n", "  scope :one do\n    query \"one\"\n    response shape: :list\n  end\n"), Some((8, "expects a list"))),
        ("aliased root needs its key", model("    attribute :title, :string\n", "  scope :mine do\n    query \"mine\"\n    response from: \"items\"\n    paginate size: 20, direction: :forward\n  end\n"), Some((7, "must start with the first field `mine`"))),
        ("aliased root with its key", model("    attribute :title, :string\n", "  scope :mine do\n    query \"mine\"\n    paginate size: 20, direction: :forward\n  end\n"), None),
        ("id from the parent object", "model Item do\n  attribute :id, :id, id_from: :owner\n  attribute :ownerId, :id, null: true\n  belongs_to :owner, class_name: Item, from: \"owner\"\n  scope :one do\n    query \"one\"\n  end\nend\n".to_string(), None),
        ("association path of the wrong shape", "model Item do\n  attribute :title, :string\n  attribute :ownerId, :id, null: true\n  has_many :owners, class_name: Item, foreign_key: :ownerId, from: \"owner\"\n  scope :one do\n    query \"one\"\n  end\nend\n".to_string(), Some((4, "expects a list"))),
        ("default covers a nullable schema field", model("    attribute :note, :string, default: \"\"\n", REMOTE), None),
        ("association path through a list", "model Item do\n  attribute :title, :string\n  attribute :ownerIds, [:id], default: []\n  references :owners, class_name: Item, foreign_key: :ownerIds, from: \"links.owner\"\n  scope :one do\n    query \"one\"\n  end\nend\n".to_string(), None),
        ("local model has no schema", "model Item, storage: :local do\n  attribute :ghost, :string\nend\n".to_string(), None),
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
    let notes = run(&model("    attribute :title, :string\n", REMOTE)).unwrap();
    assert!(notes.iter().any(|note| note.contains("model `Item` scope `all` does not use") && note.contains("extra")), "{notes:?}");
}

fn with_lines(lines: &str) -> String {
    model("    attribute :title, :string\n", lines)
}

fn parameters(args: &str) -> String {
    args.split(", ").map(|arg| {
        let (name, kind) = arg.split_once(": :").unwrap();
        let (kind, null) = kind.strip_suffix('?').map_or((kind, ""), |kind| (kind, ", null: true"));
        let kind = if kind == "ids" { "[:id]".to_string() } else { format!(":{kind}") };
        format!("    parameter :{name}, {kind}{null}\n")
    }).collect()
}

fn action_with(lines: &str) -> String {
    with_lines(&format!("  operation :tag do\n    mutation \"TagItem\"\n    optimistic false\n{lines}\n  end\n"))
}

#[test]
fn args_are_checked_against_the_input_of_their_mutation() {
    let wrapped = |args: &str| with_lines(&format!("  operation :create do\n    mutation \"CreateItem\"\n{}  end\n", parameters(args)));
    let renamed = |args: &str, variables: &str| with_lines(&format!("  operation :create do\n    mutation \"CreateItem\"\n    variables {variables}\n{}  end\n", parameters(args)));
    let cases: [SchemaCase; 14] = [
        ("string for String!", wrapped("file: :string"), None),
        ("nullable arg for a nullable input", wrapped("note: :string?, file: :string"), None),
        ("a required arg may fill a nullable input", wrapped("note: :string, file: :string"), None),
        ("id arg for an Int input", renamed("momentId: :id?, f: :string", "file: :f"), None),
        ("id arg for an ID input", renamed("ownerId: :id?, f: :string", "file: :f"), None),
        ("ids arg for [ID!]", renamed("ids: :ids?, f: :string", "file: :f"), None),
        ("input with a default is optional", renamed("withDefault: :string?, f: :string", "file: :f"), None),
        ("variable the input type lacks", renamed("g: :string, file: :string", "ghost: :g"), Some((8, "`ghost` is not an input of `ItemInput`"))),
        ("kind that does not fit", wrapped("file: :bool"), Some((6, "parameter `file` of kind `:bool` does not fit `file: String!`"))),
        ("string for an Int input", renamed("count: :string?, f: :string", "file: :f"), Some((6, "does not fit `count: Int`"))),
        ("nullable arg for a required input", wrapped("file: :string?"), Some((6, "parameter `file` may be nil but `file: String!` is required"))),
        ("operation that is not in the documents", with_lines("operation :create do\n  mutation \"Nope\"\n  parameter :a, :string\nend\n"), Some((6, "operation `Nope` is not in the operation documents"))),
        ("unwrapped action takes the variables", action_with("    parameter :label, :string, null: true\nparameter :size, :int\n    variables itemId: :id"), None),
        ("unwrapped action with a wrong kind", action_with("    parameter :size, :string\n    variables itemId: :id"), Some((6, "parameter `size` of kind `:string` does not fit `size: Int!`"))),
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
fn an_optimistic_map_covers_every_value_of_its_schema_enum() {
    let kinded = |map: &str| with_lines(&format!("  operation :kinded do\n    mutation \"KindItem\"\n    parameter :kind, Kind\n    variables itemId: :id\n    optimistic title: {{ from: :kind, map: {{ {map} }} }}\n  end\n"));
    let cases: [SchemaCase; 3] = [
        ("every value mapped", kinded("A: \"a\", B: \"b\""), None),
        ("a value left out", kinded("A: \"a\""), Some((6, "the map does not cover B of `Kind`"))),
        ("a key the enum lacks", kinded("A: \"a\", B: \"b\", C: \"c\""), Some((6, "C is not a value of `Kind`"))),
    ];
    for (name, source, expected) in cases {
        match (run(&source), expected) {
            (Ok(_), None) => {}
            (Ok(_), Some(_)) => panic!("{name}: accepted"),
            (Err(errors), None) => panic!("{name}: {errors:?}"),
            (Err(errors), Some((_, message))) => assert!(errors.iter().any(|(_, text)| text.contains(message)), "{name}: {errors:?}"),
        }
    }
}
