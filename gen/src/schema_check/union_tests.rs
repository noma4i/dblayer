use std::fs;
use std::path::PathBuf;

use crate::check_models_against_schema;

const SCHEMA: &str = r#"
type Query { items: ItemConnection! }
type ItemConnection { nodes: [Item!]! pageInfo: PageInfo! }
type PageInfo { hasNextPage: Boolean! }
union Attachment = Gift | Moment
type Gift { id: ID! tier: String! }
type Moment { momentId: ID! caption: String }
type Owner { id: ID! }
type Item { id: ID! owner: Owner attachment: Attachment note: String }
"#;

const SELECTION: &str = "{ id attachment { __typename ... on Gift { id tier } ... on Moment { momentId } } }";

const VARIANTS: &str = "    on Gift do\n      field :id, type: :id\n      field :tier, type: :string\n    end\n    on Moment do\n      field :momentId, type: :id\n      field :caption, type: :string, null: true\n    end\n";

fn model(attachment: &str) -> String {
    format!("model Item do\n\n{attachment}\n  scope :all do\n    fetch \"items\"\n    paginate 20\n  end\nend\n")
}

fn good(body: &str) -> String {
    model(&format!("  field :attachment, type: Attachment, null: true do\n{body}  end\n"))
}

fn run(source: &str, document: &str) -> Result<(), Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let schema = directory.path().join("schema.graphql");
    fs::write(&schema, SCHEMA).unwrap();
    fs::create_dir(directory.path().join("queries")).unwrap();
    fs::write(directory.path().join("queries/items.graphql"), document).unwrap();
    let file = directory.path().join("item.dbl");
    fs::write(&file, source).unwrap();
    let operations: Vec<PathBuf> = vec![directory.path().join("queries")];
    check_models_against_schema(&schema, &operations, &[file]).map(|_| ()).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

#[test]
fn union_declarations_are_checked_against_the_schema_union() {
    let cases: [(&str, String, Option<&str>); 6] = [
        ("every variant fits", good(VARIANTS), None),
        ("variant outside the union", good(&format!("{VARIANTS}    on Owner do\n      field :id, type: :id\n    end\n")), Some("`Owner` is not a member of union `Attachment`")),
        ("union member without a variant", good(&VARIANTS[..VARIANTS.find("    on Moment").unwrap()]), Some("variant `Moment` of union `Attachment` is not declared")),
        ("variant field the schema type lacks", good(&VARIANTS.replace("field :tier, type: :string", "field :ghost, type: :string")), Some("type `Gift` has no field `ghost`")),
        ("nullable union declared required", model(&format!("  field :attachment, type: Attachment do\n{VARIANTS}  end\n")), Some("declare `null: true`")),
        ("union declared over an object type", model(&format!("  field :owner, type: Attachment, null: true do\n{VARIANTS}  end\n")), Some("is not a union")),
    ];
    for (name, source, expected) in cases {
        match (run(&source, &format!("query items {{ items {{ nodes {SELECTION} }} }}\n")), expected) {
            (Ok(()), None) => {}
            (Ok(()), Some(_)) => panic!("{name}: accepted"),
            (Err(errors), None) => panic!("{name}: {errors:?}"),
            (Err(errors), Some(message)) => assert!(errors.iter().any(|text| text.contains(message)), "{name}: {errors:?}"),
        }
    }
}

#[test]
fn the_operation_selection_of_a_union_field_matches_the_declared_variants() {
    let cases: [(&str, &str, Option<&str>); 5] = [
        ("typename and declared fragments", SELECTION, None),
        ("field not selected", "{ id }", None),
        ("typename missing", "{ id attachment { ... on Gift { id tier } ... on Moment { momentId } } }", Some("must select `__typename`")),
        ("fragment on an undeclared type", "{ id attachment { __typename ... on Gift { id } ... on Owner { id } } }", Some("`... on Owner` is not a declared variant")),
        ("named fragment on an undeclared type", "{ id attachment { __typename ...Piece } }\nfragment Piece on Attachment { ... on Owner { id } }", Some("`... on Owner` is not a declared variant")),
    ];
    for (name, selection, expected) in cases {
        let (body, fragments) = selection.split_once('\n').unwrap_or((selection, ""));
        let result = run(&good(VARIANTS), &format!("query items {{ items {{ nodes {body} }} }}\n{fragments}\n"));
        match (result, expected) {
            (Ok(()), None) => {}
            (Ok(()), Some(_)) => panic!("{name}: accepted"),
            (Err(errors), None) => panic!("{name}: {errors:?}"),
            (Err(errors), Some(message)) => assert!(errors.iter().any(|text| text.contains(message)), "{name}: {errors:?}"),
        }
    }
}
