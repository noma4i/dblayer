use std::fs;

use crate::{compile_files, emit_kotlin, emit_swift, Declarations};

const SCHEMA: &str = r#"
type Query { chat(id: ID!): Chat, node(id: ID!): Chat }
scalar Cursor
type Chat { id: ID! title: String! messages(first: Int, after: Cursor, afterSequence: Int, includeGifts: Boolean): MessageConnection! mediaLibrary(upToSequence: Int): MediaLibrary! }
type MediaLibrary { visualCount: Int! audioCount: Int! giftCount: Int! latestSequence: Int }
type MessageConnection { edges: [MessageEdge!]! pageInfo: PageInfo! }
type MessageEdge { cursor: Cursor! node: Message! }
type Message { id: ID! text: String }
type PageInfo { hasNextPage: Boolean! endCursor: Cursor }
"#;

const OPERATIONS: &str = "query messagesSince($chatId: ID!, $afterSequence: Int, $includeGifts: Boolean, $first: Int, $after: Cursor) { chat(id: $chatId) { messages(first: $first, after: $after, afterSequence: $afterSequence, includeGifts: $includeGifts) { edges { cursor node { id text } } pageInfo { hasNextPage endCursor } } } }\nquery chatMediaState($chatId: ID!, $upToSequence: Int) { node(id: $chatId) { ... on Chat { mediaLibrary(upToSequence: $upToSequence) { visualCount audioCount giftCount latestSequence } } } }\n";

const MODEL: &str = "model Message do\n  attribute :text, :string, null: true\n  scope :since do\n    parameter :chatId, :string\n    parameter :afterSequence, :int\n    parameter :includeGifts, :bool\n    query \"messagesSince\"\n    response from: \"chat.messages\"\n    paginate size: 50, direction: :forward\n  end\nend\n";

const MEDIA_STATE: &str = "operation :chat_media_state do\n  query \"chatMediaState\"\n  parameter :chatId, :id\n  parameter :upToSequence, :int, null: true\n  returns do\n    attribute :mediaLibrary, ChatMediaStateResultMediaLibrary do\n      attribute :visualCount, :int\n      attribute :audioCount, :int\n      attribute :giftCount, :int\n      attribute :latestSequence, :int\n    end\n  end\nend\n";

fn compile(source: &str, schema: Option<&str>) -> Result<Declarations, Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("message.dbl");
    fs::write(&file, source).unwrap();
    let schema_file = schema.map(|text| {
        let path = directory.path().join("schema.graphql");
        fs::write(&path, text).unwrap();
        fs::create_dir(directory.path().join("queries")).unwrap();
        fs::write(directory.path().join("queries/ops.graphql"), OPERATIONS).unwrap();
        path
    });
    let documents = vec![directory.path().join("queries")];
    compile_files(&[file], schema_file.as_deref(), if schema_file.is_some() { &documents } else { &[] }).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

#[test]
fn typed_scope_params_are_int_and_bool_arguments_on_both_platforms() {
    let declared = compile(MODEL, Some(SCHEMA)).unwrap();
    let relation = &declared.models[0].relations[0];
    assert_eq!(relation.text_params, ["chatId", "afterSequence", "includeGifts"]);
    assert_eq!(relation.scalar_types.len(), 2);
    let swift = emit_swift(&declared.models, &declared.operations, &declared.inputs);
    for line in [
        "public static func since(_ chatId: String, afterSequence: Int, includeGifts: Bool) throws -> Query<Self> {",
        "params: [\"chatId\": try JSONValue.encode(chatId), \"afterSequence\": try JSONValue.encode(afterSequence), \"includeGifts\": try JSONValue.encode(includeGifts)]",
    ] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &declared.inputs);
    for line in ["fun since(chatId: String, afterSequence: Long, includeGifts: Boolean): Query<Message> {", "put(\"afterSequence\", jsonValue(afterSequence))"] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
}

#[test]
fn typed_params_name_only_int_or_bool() {
    let source = MODEL.replace("parameter :afterSequence, :int", "parameter :afterSequence, :num");
    let errors = compile(&source, None).unwrap_err();
    assert!(errors.iter().any(|message| message.contains("a server-only parameter is")), "{errors:?}");
}

#[test]
fn a_typed_param_must_fit_the_variable_of_the_document() {
    let wrong = MODEL.replace("parameter :includeGifts, :bool", "parameter :includeGifts, :int");
    let errors = compile(&wrong, Some(SCHEMA)).unwrap_err();
    assert!(errors.iter().any(|message| message.contains("includeGifts") && message.contains("Boolean")), "{errors:?}");
    let missing = MODEL.replace("parameter :afterSequence, :int", "parameter :since, :int");
    let errors = compile(&missing, Some(SCHEMA)).unwrap_err();
    assert!(errors.iter().any(|message| message.contains("no variable `$since`")), "{errors:?}");
}

#[test]
fn an_aggregate_query_is_a_fetch_only_call_that_returns_its_fields() {
    let declared = compile(&format!("{MODEL}\n{MEDIA_STATE}"), Some(SCHEMA)).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &declared.inputs);
    for line in ["public static func chatMediaState(chatId: String, upToSequence: Int? = nil) async throws", "public struct ChatMediaStateResultMediaLibrary"] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &declared.inputs);
    assert!(kotlin.contains("suspend fun chatMediaState(chatId: String, upToSequence: Long? = null)"), "{kotlin}");
}
