use std::fs;

use crate::compile_files;

const SCHEMA: &str = r#"
type Query { documentBySlug(slug: String!, source: Source): [Document!]! }
type Subscription { documentUpdated(slug: String!, source: Source, tenant: ID, token: ID!): Document }
enum Source { WEB MOBILE }
type Document { id: ID! slug: String! }
"#;

const MODEL: &str = "model Document do\n  attribute :slug, :string\n  scope :by_slug do\n    parameter :slug, :string\n    parameter :source, Source, default: \"WEB\"\n    where slug: :slug\n    query \"documentBySlug\"\n    response from: \"documentBySlug\", shape: :list\n    subscribe \"documentUpdated\"\n  end\nend\n";

fn compile(subscription: &str) -> Result<(), Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("document.dbl");
    fs::write(&file, MODEL).unwrap();
    let schema = directory.path().join("schema.graphql");
    fs::write(&schema, SCHEMA).unwrap();
    fs::create_dir(directory.path().join("queries")).unwrap();
    let operations = format!("query documentBySlug($slug: String!, $source: Source) {{ documentBySlug(slug: $slug, source: $source) {{ id slug }} }}\n{subscription}\n");
    fs::write(directory.path().join("queries/all.graphql"), operations).unwrap();
    compile_files(&[file], Some(&schema), &[directory.path().join("queries")]).map(|_| ()).map_err(|errors| errors.into_iter().map(|error| format!("{}: {}", error.error.line, error.error.message)).collect())
}

#[test]
fn subscription_arguments_must_match_the_scope_parameters() {
    let cases = [
        ("same names and types", "subscription documentUpdated($slug: String!, $source: Source) { documentUpdated(slug: $slug, source: $source) { id slug } }", None),
        ("optional scope param left out", "subscription documentUpdated($slug: String!) { documentUpdated(slug: $slug) { id slug } }", None),
        ("extra required argument", "subscription documentUpdated($slug: String!, $token: ID!) { documentUpdated(slug: $slug, token: $token) { id slug } }", Some("variable `$token` has no source")),
        ("wrong type", "subscription documentUpdated($slug: String!, $source: Int) { documentUpdated(slug: $slug, source: $source) { id slug } }", Some("param `source` is `Source` but the variable `$source` of `documentUpdated` is `Int`")),
    ];
    for (name, document, expected) in cases {
        match (compile(document), expected) {
            (Ok(()), None) => {}
            (Ok(()), Some(_)) => panic!("{name}: accepted"),
            (Err(errors), None) => panic!("{name}: {errors:?}"),
            (Err(errors), Some(message)) => assert!(errors.iter().any(|text| text.contains(message)), "{name}: {errors:?}"),
        }
    }
}
