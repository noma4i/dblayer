use std::fs;
use std::path::PathBuf;

use crate::check_models_against_schema;

const SCHEMA: &str = r#"
type Query { screens: ScreenConnection! }
type ScreenConnection { nodes: [Screen!]! pageInfo: PageInfo! }
type PageInfo { hasNextPage: Boolean! }
interface Element { id: ID! children: [ID!]! note: String }
type Box implements Element { id: ID! children: [ID!]! note: String gap: Int }
type Text implements Element { id: ID! children: [ID!]! note: String body: String! }
type Spec { root: ID! elements: [Element!]! }
type Screen { id: ID! spec: Spec first: Pick }
union Pick = Box | Text
"#;

const SELECTION: &str = "{ id spec { root elements { __typename id children note ... on Box { gap } ... on Text { body } } } }";

const SHARED: &str = "      shared do\n        field :id, type: :id\n        field :children, type: [:id]\n        field :note, type: :string, null: true\n      end\n";

const VARIANTS: &str = "      on Box do\n        field :gap, type: :int, null: true\n      end\n      on Text do\n        field :body, type: :string\n      end\n";

fn model(spec: &str) -> String {
    format!("model Screen do\n  field :spec, type: Spec, null: true do\n    field :root, type: :id\n{spec}  end\n  scope :all do\n    fetch \"screens\"\n    paginate 20\n  end\nend\n")
}

fn elements(body: &str) -> String {
    model(&format!("    field :elements, type: [Element] do\n{body}    end\n"))
}

fn run(source: &str, schema: &str, selection: &str) -> Result<(), Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let schema_file = directory.path().join("schema.graphql");
    fs::write(&schema_file, schema).unwrap();
    fs::create_dir(directory.path().join("queries")).unwrap();
    fs::write(directory.path().join("queries/screens.graphql"), format!("query screens {{ screens {{ nodes {selection} }} }}\n")).unwrap();
    let file = directory.path().join("screen.dbl");
    fs::write(&file, source).unwrap();
    let operations: Vec<PathBuf> = vec![directory.path().join("queries")];
    check_models_against_schema(&schema_file, &operations, &[file]).map(|_| ()).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

#[test]
fn interface_declarations_are_checked_against_the_schema_interface_and_its_implementations() {
    let without_text = &VARIANTS[..VARIANTS.find("      on Text").unwrap()];
    let lacking = SCHEMA.replace("type Text implements Element { id: ID! children: [ID!]! note: String body: String! }", "type Text implements Element { id: ID! note: String body: String! }");
    let cases: [(&str, String, &str, &str, Option<&str>); 9] = [
        ("every implementation fits", elements(&format!("{SHARED}{VARIANTS}")), SCHEMA, SELECTION, None),
        ("implementation without a variant", elements(&format!("{SHARED}{without_text}")), SCHEMA, SELECTION, Some("implementation `Text` of interface `Element` is not declared")),
        ("variant that is not an implementation", elements(&format!("{SHARED}{VARIANTS}      on Screen do\n        field :id, type: :id\n      end\n")), SCHEMA, SELECTION, Some("variant `Screen` is not an implementation of interface `Element`")),
        ("shared field the interface lacks", elements(&format!("{}{VARIANTS}", SHARED.replace("field :note, type: :string, null: true", "field :ghost, type: :string"))), SCHEMA, SELECTION, Some("type `Element` has no field `ghost`")),
        ("shared field an implementation lacks", elements(&format!("{SHARED}{VARIANTS}")), &lacking, SELECTION, Some("implementation `Text` of interface `Element` has no field `children`")),
        ("shared nullable field declared required", elements(&format!("{}{VARIANTS}", SHARED.replace(", null: true", ""))), SCHEMA, SELECTION, Some("declare `null: true`")),
        ("selection without __typename", elements(&format!("{SHARED}{VARIANTS}")), SCHEMA, &SELECTION.replace("__typename ", ""), Some("must select `__typename`")),
        ("spread of an undeclared implementation", elements(&format!("{SHARED}{VARIANTS}")), SCHEMA, &SELECTION.replace("... on Box { gap }", "... on Screen { id }"), Some("`... on Screen` is not a declared variant of interface `Element`")),
        ("shared block over a plain union", format!("model Screen do\n  field :first, type: Pick, null: true do\n{SHARED}{VARIANTS}  end\n  scope :all do\n    fetch \"screens\"\n    paginate 20\n  end\nend\n"), SCHEMA, "{ id first { __typename } }", Some("`shared` needs an interface: `Pick` is a union")),
    ];
    for (name, source, schema, selection, expected) in cases {
        match (run(&source, schema, selection), expected) {
            (Ok(()), None) => {}
            (Ok(()), Some(_)) => panic!("{name}: accepted"),
            (Err(errors), None) => panic!("{name}: {errors:?}"),
            (Err(errors), Some(message)) => assert!(errors.iter().any(|text| text.contains(message)), "{name}: {errors:?}"),
        }
    }
}
