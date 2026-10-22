use std::fs;
use std::path::PathBuf;

use crate::check_models_against_schema;

const SCHEMA: &str = r#"
type Query { chat(id: ID!): Chat }
type Chat { id: ID! messages(first: Int, after: String): MessageConnection! }
type Message { id: ID! text: String! }
type MessageConnection { edges: [MessageEdge!]! pageInfo: PageInfo! }
type MessageEdge { node: Message! cursor: String! sequenceNumber: Int! impressionUuid: String impressionScore: Int }
type PageInfo { hasNextPage: Boolean! endCursor: String }
"#;

const MESSAGE: &str = "model Message do\n  attribute :text, :string\n  scope :thread do\n    query \"ChatThread\"\n    response from: \"chat.messages\"\n    paginate size: 30, direction: :forward\n    EDGES\n  end\nend\n";

const DOCUMENT: &str = "query ChatThread($first: Int, $after: String) { chat(id: \"c\") { messages(first: $first, after: $after) { edges { EDGE node { id text } } pageInfo { hasNextPage endCursor } } } }\n";

fn run(edges: &str, edge: &str) -> Result<(), Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let schema = directory.path().join("schema.graphql");
    fs::write(&schema, SCHEMA).unwrap();
    fs::create_dir(directory.path().join("queries")).unwrap();
    fs::write(directory.path().join("queries/all.graphql"), DOCUMENT.replace("EDGE", edge)).unwrap();
    let model: PathBuf = directory.path().join("message.dbl");
    fs::write(&model, MESSAGE.replace("EDGES", edges)).unwrap();
    check_models_against_schema(&schema, &[directory.path().join("queries")], &[model]).map(|_| ()).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

#[test]
fn edges_are_checked_against_the_edge_type_of_the_connection_of_their_scope() {
    let uuid = "edge do\n      attribute :impressionUuid, :string, null: true\n    end";
    let cases: [(&str, String, &str, Option<&str>); 7] = [
        ("string and int edge fields", "edge do\n      attribute :impressionUuid, :string, null: true\n      attribute :impressionScore, :int, null: true\n    end".to_string(), "impressionUuid impressionScore", None),
        ("a key the edge type does not have", "edge do\n      attribute :ghost, :string, null: true\n    end".to_string(), "cursor", Some("ghost")),
        ("an edge the document does not select", uuid.to_string(), "cursor", Some("does not select `impressionUuid`")),
        ("a string for an Int edge field", "edge do\n      attribute :impressionScore, :string, null: true\n    end".to_string(), "impressionScore", Some("the edge field is `Int`")),
        ("an Int edge field declared as num", "edge do\n      attribute :impressionScore, :num, null: true\n    end".to_string(), "impressionScore", Some("the edge field is `Int`")),
        ("a nullable edge field declared without null", "edge do\n      attribute :impressionUuid, :string, optional: false\n    end".to_string(), "impressionUuid", Some("null: true")),
        ("an aliased edge field", "edge do\n      attribute :impressionUuid, :string, null: true, from: \"seen\"\n    end".to_string(), "seen: impressionUuid", None),
    ];
    for (name, edges, edge, expected) in cases {
        match (run(&edges, edge), expected) {
            (Ok(()), None) => {}
            (Ok(()), Some(_)) => panic!("{name}: accepted"),
            (Err(errors), None) => panic!("{name}: {errors:?}"),
            (Err(errors), Some(message)) => assert!(errors.iter().any(|text| text.contains(message)), "{name}: {errors:?}"),
        }
    }
}
