use std::fs;
use std::path::PathBuf;

use crate::check_models_against_schema;

const SCHEMA: &str = r#"
type Query { items: ItemConnection! node(id: ID!): Node }
interface Node { id: ID! }
type ItemConnection { nodes: [Item!]! pageInfo: PageInfo! }
type PageInfo { hasNextPage: Boolean! }
union Attachment = Gift | Moment
type Gift { id: ID! kind: String! }
type Moment { momentId: ID! kind: String! }
interface Part { id: ID! kind: String! }
type Wing implements Part { id: ID! kind: String! span: Int }
type Rotor implements Part { id: ID! kind: String! }
union Text = Literal | Reference
type Literal { text: String! }
type Reference { path: String! }
type Owner { id: ID! name: String! }
type Detail { label: String! }
type Item implements Node { id: ID! title: String owner: Owner attachment: Attachment parts: [Part!]! name: Text details: [Detail!]! }
"#;

const ATTACHMENT: &str = "  field :attachment, type: Attachment, null: true do\n    on Gift do\n      field :id, type: :id\n      field :kind, type: :string, from: \"giftTier\"\n    end\n    on Moment do\n      field :momentId, type: :id\n      field :kind, type: :string, from: \"momentKind\"\n    end\n  end\n";

const PARTS: &str = "  field :parts, type: [Part] do\n    shared do\n      field :id, type: :id\n      field :category, type: :string, from: \"partKind\"\n    end\n    on Wing do\n      field :span, type: :int, null: true\n    end\n    on Rotor do\n    end\n  end\n";

const NAME: &str = "  field :name, type: Text, null: true do\n    on Literal do\n      field :text, type: :string\n    end\n    on Reference do\n      field :path, type: :string\n    end\n  end\n";

const TEXT_PIECE: &str = "fragment TextPiece on Text { __typename ... on Literal { text } ... on Reference { path } }";

fn model(body: &str) -> String {
    format!("model Item do\n\n{body}  scope :all do\n    fetch \"items\"\n    paginate 20\n  end\nend\n")
}

fn run(source: &str, selection: &str, fragments: &str) -> Result<(), Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let schema = directory.path().join("schema.graphql");
    fs::write(&schema, SCHEMA).unwrap();
    fs::create_dir(directory.path().join("queries")).unwrap();
    fs::write(directory.path().join("queries/items.graphql"), format!("query items {{ items {{ nodes {selection} }} }}\n{fragments}\n")).unwrap();
    let file = directory.path().join("item.dbl");
    fs::write(&file, source).unwrap();
    let operations: Vec<PathBuf> = vec![directory.path().join("queries")];
    check_models_against_schema(&schema, &operations, &[file]).map(|_| ()).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

fn expect(cases: Vec<(&str, String, &str, &str, Option<&str>)>) {
    let mut failed = Vec::new();
    for (name, source, selection, fragments, expected) in cases {
        match (run(&source, selection, fragments), expected) {
            (Ok(()), None) => {}
            (Ok(()), Some(_)) => failed.push(format!("{name}: accepted")),
            (Err(errors), None) => failed.push(format!("{name}: {errors:?}")),
            (Err(errors), Some(message)) => {
                if !errors.iter().any(|text| text.contains(message)) {
                    failed.push(format!("{name}: {errors:?}"));
                }
            }
        }
    }
    assert!(failed.is_empty(), "{}", failed.join("\n"));
}

#[test]
fn from_names_the_response_key_of_the_operation() {
    let variants = "{ id attachment { __typename ... on Gift { id giftTier: kind } ... on Moment { momentId momentKind: kind } } }";
    let association = "model Item do\n\n  field :title, type: :string, null: true\n  field :ownerId, type: :id, null: true\n\n  belongs_to :owner, model: Item, from: \"ownerRef\"\n  scope :all do\n    fetch \"items\"\n    paginate 20\n  end\nend\n";
    expect(vec![
        ("top level field", model("  field :caption, type: :string, null: true, from: \"heading\"\n"), "{ id heading: title }", "", None),
        ("nested object", model("  field :owner, type: ItemOwner, null: true do\n    field :ownerName, type: :string, from: \"label\"\n  end\n"), "{ id owner { label: name } }", "", None),
        ("nested list", model("  field :details, type: [ItemDetail] do\n    field :text, type: :string, from: \"caption\"\n  end\n"), "{ id details { caption: label } }", "", None),
        ("union variants", model(ATTACHMENT), variants, "", None),
        ("shared block of an interface", model(PARTS), "{ id parts { __typename id partKind: kind ... on Wing { span } } }", "", None),
        ("association root", association.to_string(), "{ id ownerRef: owner { id } }", "", None),
        ("alias named in no document", model(ATTACHMENT), "{ id attachment { __typename ... on Gift { id kind } ... on Moment { momentId kind } } }", "", Some("type `Gift` has no field `giftTier`")),
        ("alias of another variant", model(ATTACHMENT), "{ id attachment { __typename ... on Gift { id kind } ... on Moment { momentId momentKind: kind giftTier: kind } } }", "", Some("type `Gift` has no field `giftTier`")),
        ("operation of another scope that leaves the field out", model(&format!("  node ItemBrief\n{ATTACHMENT}")), variants, "query ItemBrief($id: ID!) { node(id: $id) { ... on Item { id title } } }", None),
        ("alias through a named fragment", model(ATTACHMENT), "{ id attachment { __typename ...Pieces } }", "fragment Pieces on Attachment { ... on Gift { id giftTier: kind } ... on Moment { momentId momentKind: kind } }", None),
    ]);
}

#[test]
fn the_typename_of_a_union_is_found_in_the_whole_selection() {
    expect(vec![
        ("typename through a fragment", model(NAME), "{ id name { ...TextPiece } owner { name } }", TEXT_PIECE, None),
        ("typename through a nested fragment", model(NAME), "{ id name { ...Outer } }", "fragment Outer on Text { ...TextPiece }\nfragment TextPiece on Text { __typename ... on Literal { text } ... on Reference { path } }", None),
        ("typename in an inline fragment", model(NAME), "{ id name { ... on Text { __typename } ... on Literal { text } ... on Reference { path } } }", "", None),
        ("fragments that spread each other", model(NAME), "{ id name { ...Left } }", "fragment Left on Text { ...Right }\nfragment Right on Text { ...Left __typename ... on Literal { text } ... on Reference { path } }", None),
        ("fragment without typename", model(NAME), "{ id name { ...Bare } owner { name } }", "fragment Bare on Text { ... on Literal { text } ... on Reference { path } }", Some("must select `__typename`")),
        ("spread of a fragment no document declares", model(NAME), "{ id name { ...Missing } }", "", Some("fragment `Missing` is not in the operation documents")),
        ("same name on another type", model(NAME), "{ id owner { name } name { ... on Literal { text } ... on Reference { path } } }", "", Some("must select `__typename`")),
    ]);
}
