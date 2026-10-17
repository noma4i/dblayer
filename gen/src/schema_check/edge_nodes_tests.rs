use std::fs;
use std::path::PathBuf;

use crate::check_models_against_schema;

const SCHEMA: &str = r#"
type Query { chat(id: ID!): Chat }
type Chat { id: ID! messages(first: Int, after: String): MessageConnection! }
type Message { id: ID! text: String! }
type MessageConnection { nodes: [Message!]! edges: [MessageEdge!]! pageInfo: PageInfo! }
type MessageEdge { node: Message! cursor: String! impressionUuid: String }
type PageInfo { hasNextPage: Boolean! endCursor: String }
"#;

const MESSAGE: &str = "model Message do\n  field :text, type: :string\n  scope :thread do\n    fetch ChatThread, root: \"chat.messages\"\n    paginate 30\n    EDGES\n  end\nend\n";

const DOCUMENT: &str = "query ChatThread($first: Int, $after: String) { chat(id: \"c\") { messages(first: $first, after: $after) { edges { EDGE node { id text } } pageInfo { hasNextPage endCursor } } } }\n";

const NODES_DOCUMENT: &str = "query ChatThread($first: Int, $after: String) { chat(id: \"c\") { messages(first: $first, after: $after) { nodes { id text } pageInfo { hasNextPage endCursor } } } }\n";

fn run(edges: &str, edge: &str) -> Result<(), Vec<String>> {
    run_document(edges, &DOCUMENT.replace("EDGE", edge))
}

fn run_document(edges: &str, document: &str) -> Result<(), Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let schema = directory.path().join("schema.graphql");
    fs::write(&schema, SCHEMA).unwrap();
    fs::create_dir(directory.path().join("queries")).unwrap();
    fs::write(directory.path().join("queries/all.graphql"), document).unwrap();
    let model: PathBuf = directory.path().join("message.dbl");
    fs::write(&model, MESSAGE.replace("EDGES", edges)).unwrap();
    check_models_against_schema(&schema, &[directory.path().join("queries")], &[model]).map(|_| ()).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

#[test]
fn a_connection_with_nodes_and_edges_in_its_schema_checks_the_edges_the_document_selects() {
    run("edge :impressionUuid, type: :string, null: true", "impressionUuid").unwrap();
    let errors = run("edge :typo, type: :string, null: true", "cursor").unwrap_err();
    assert!(errors.iter().any(|text| text.contains("typo")), "{errors:?}");
    let errors = run("edge :impressionUuid, type: :int, null: true", "impressionUuid").unwrap_err();
    assert!(errors.iter().any(|text| text.contains("does not fit the schema type `String`")), "{errors:?}");
}

#[test]
fn an_edge_needs_a_document_that_selects_the_edges_even_when_the_schema_has_nodes() {
    run_document("", NODES_DOCUMENT).unwrap();
    let errors = run_document("edge :impressionUuid, type: :string, null: true", NODES_DOCUMENT).unwrap_err();
    assert!(errors.iter().any(|text| text.contains("does not select `edges`")), "{errors:?}");
}
