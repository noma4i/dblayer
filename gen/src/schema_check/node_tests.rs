use std::fs;
use std::path::PathBuf;

use crate::check_models_against_schema;

const SCHEMA: &str = r#"
type Query { node(id: ID!): Node nodes(ids: [ID!]!): [Node]! }
interface Node { id: ID! }
type Chat implements Node { id: ID! title: String! shareUrl: String messages(first: Int, after: String): MessageConnection! }
type Message implements Node { id: ID! chatId: ID! text: String! media: Media }
type Media { kind: String! fileUrl: String }
type Gadget { id: ID! }
type MessageConnection { edges: [MessageEdge!]! pageInfo: PageInfo! }
type MessageEdge { node: Message! }
type PageInfo { hasNextPage: Boolean! endCursor: String }
"#;

const CHAT: &str = "model Chat do\n  attribute :title, :string\n  attribute :shareUrl, :string, null: true\n  scope :node, node: :one do\n    parameter :id, :id\n    query \"ChatNode\"\n    variables id: :id\n  end\n  scope :nodes, node: :many do\n    query \"ChatsByIds\"\n    response shape: :list\n  end\nend\n";

const MESSAGE: &str = "model Message do\n  attribute :chatId, :id\n  attribute :text, :string\n  attribute :media, MessageMedia, null: true do\n    attribute :kind, :string\n    attribute :fileUrl, :string, null: true\n  end\n  scope :thread do\n    parameter :chatId, :id\n    where chatId: :chatId\n    query \"ChatThread\"\n    response from: \"node.messages\"\n    paginate size: 30, direction: :forward\n  end\n  scope :status, node: :one do\n    parameter :id, :id\n    query \"MessageStatus\"\n    variables id: :id\n  end\nend\n";

const OPERATIONS: &str = "query ChatNode($id: ID!) { node(id: $id) { ... on Chat { id title shareUrl } } }
query ChatsByIds($ids: [ID!]!) { nodes(ids: $ids) { ... on Chat { id title shareUrl } } }
query ChatThread($chatId: ID!, $first: Int, $after: String) { node(id: $chatId) { ... on Chat { messages(first: $first, after: $after) { edges { node { id chatId text media { kind fileUrl } } } pageInfo { hasNextPage endCursor } } } } }
query MessageStatus($id: ID!) { node(id: $id) { ... on Message { id chatId media { kind fileUrl } } } }
";

fn run(models: &[&str], operations: &str) -> Result<(), Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let schema = directory.path().join("schema.graphql");
    fs::write(&schema, SCHEMA).unwrap();
    fs::create_dir(directory.path().join("queries")).unwrap();
    fs::write(directory.path().join("queries/all.graphql"), operations).unwrap();
    let files: Vec<PathBuf> = models.iter().enumerate().map(|(at, source)| {
        let file = directory.path().join(format!("model{at}.dbl"));
        fs::write(&file, source).unwrap();
        file
    }).collect();
    check_models_against_schema(&schema, &[directory.path().join("queries")], &files).map(|_| ()).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

#[test]
fn node_reads_are_checked_through_the_type_under_the_inline_fragment() {
    let replaced = |from: &str, to: &str| OPERATIONS.replace(from, to);
    let cases: [(&str, String, Option<&str>); 7] = [
        ("one read, a relation under node and a subset read", OPERATIONS.to_string(), None),
        ("a type that does not implement Node", replaced("... on Chat { id title shareUrl } } }\nquery ChatsByIds", "... on Gadget { id } } }\nquery ChatsByIds"), Some("`... on Gadget` is not an implementation of `Node`")),
        ("a field outside the fragment", replaced("node(id: $id) { ... on Chat { id title", "node(id: $id) { id ... on Chat { title"), Some("field `id` of `node` is selected outside `... on`")),
        ("two types under one node", replaced("... on Chat { id title shareUrl } } }\nquery ChatsByIds", "... on Chat { id title } ... on Message { id } } }\nquery ChatsByIds"), Some("more than one `... on` type: Chat, Message")),
        ("no fragment at all", replaced("... on Chat { id title shareUrl } } }\nquery ChatsByIds", "__typename } }\nquery ChatsByIds"), Some("select its fields under `... on <Type>`")),
        ("a named fragment on the implementation", replaced("node(id: $id) { ... on Chat { id title shareUrl } } }\nquery ChatsByIds", "node(id: $id) { ...Piece } }\nfragment Piece on Chat { id title shareUrl }\nquery ChatsByIds"), None),
        ("nodes with a type that does not implement Node", replaced("nodes(ids: $ids) { ... on Chat { id title shareUrl } }", "nodes(ids: $ids) { ... on Gadget { id } }"), Some("`... on Gadget` is not an implementation of `Node`")),
    ];
    for (name, document, expected) in cases {
        match (run(&[CHAT, MESSAGE], &document), expected) {
            (Ok(()), None) => {}
            (Ok(()), Some(_)) => panic!("{name}: accepted"),
            (Err(errors), None) => panic!("{name}: {errors:?}"),
            (Err(errors), Some(message)) => assert!(errors.iter().any(|text| text.contains(message)), "{name}: {errors:?}"),
        }
    }
}

#[test]
fn a_node_model_field_the_fragment_type_lacks_is_reported() {
    let source = CHAT.replace("attribute :title, :string\n", "attribute :title, :string\n  attribute :ghost, :string, null: true\n");
    let errors = run(&[&source, MESSAGE], OPERATIONS).unwrap_err();
    assert!(errors.iter().any(|text| text.contains("type `Chat` has no field `ghost`")), "{errors:?}");
}
