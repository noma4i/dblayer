use std::fs;

use crate::{compile_files, parse_models};

const MODEL: &str = "model Chat do\n  attribute :title, :string\n  attribute :serverRevision, :int, from: \"revision\"\n  server_revision :serverRevision\n  scope :node, node: :one do\n    parameter :id, :id\n    query \"ChatNode\"\n    variables id: :id\n  end\n  changes \"Changes\", from: \"changes.entries\", area: \"area\", kind: \"kind\", id: \"id\"\n  changed_by \"MESSAGE\", via: :node, payload: \"chat\"\nend\n";
const SCHEMA: &str = "scalar Int64\ninterface Node { id: ID! }\ntype Chat implements Node { id: ID! title: String! revision: Int64! }\ntype Message { id: ID! }\ntype Query { node(id: ID!): Node }\ntype Subscription { changes: Changes! }\ntype Changes { entries: [Change!]! }\ntype Change { area: String! kind: String! id: ID! chat: Chat message: Message }\n";
const DOCUMENTS: &str = "query ChatNode($id: ID!) { node(id: $id) { ... on Chat { id title revision } } }\nsubscription Changes { changes { entries { area kind id chat { id title revision } message { id } } } }\n";

#[test]
fn event_payload_declarations_check_paths_types_fields_and_revision() {
    for (replace, expected) in [
        ("server_revision :absent", "server_revision requires a required integer attribute"),
        ("server_revision :title", "server_revision requires a required integer attribute"),
        ("server_revision :serverRevision\n  server_revision :serverRevision", "duplicate server_revision"),
    ] {
        let errors = parse_models(&MODEL.replace("server_revision :serverRevision", replace)).unwrap_err();
        assert!(errors.iter().any(|error| error.message.contains(expected)), "{errors:?}");
    }
    let directory = tempfile::tempdir().unwrap();
    let model = directory.path().join("chat.dbl");
    let schema = directory.path().join("schema.graphql");
    let documents = directory.path().join("operations.graphql");
    fs::write(&schema, SCHEMA).unwrap();
    for (source, operations, expected) in [
        (MODEL.to_string(), DOCUMENTS.to_string(), None),
        (MODEL.replace("payload: \"chat\"", "payload: \"absent\""), DOCUMENTS.to_string(), Some("has no `absent`")),
        (MODEL.replace("payload: \"chat\"", "payload: \"message\""), DOCUMENTS.to_string(), Some("expected model `Chat`")),
        (MODEL.replace("payload: \"chat\"", "payload: \"chat\", id_of: { of: Chat, field: :id }"), DOCUMENTS.to_string(), Some("without id_of")),
        (MODEL.to_string(), DOCUMENTS.replace("chat { id title revision }", "chat { id title }"), Some("payload field `revision`")),
        (MODEL.to_string(), DOCUMENTS.replace("chat { id title revision }", "chat { title revision }"), Some("payload id `id`")),
    ] {
        fs::write(&model, format!("scalar :Int64, as: :long\n{source}")).unwrap();
        fs::write(&documents, operations).unwrap();
        let result = compile_files(&[&model], Some(&schema), std::slice::from_ref(&documents));
        match expected {
            Some(expected) => {
                let errors = result.unwrap_err();
                assert!(errors.iter().any(|error| error.error.message.contains(expected)), "expected {expected}: {errors:?}");
            }
            None => { result.unwrap(); }
        }
    }
}

#[test]
fn two_models_of_one_area_keep_their_own_payload_and_revision() {
    let message = "model Message do\n  attribute :text, :string\n  scope :node, node: :one do\n    parameter :id, :id\n    query \"MessageNode\"\n    variables id: :id\n  end\n  changed_by \"MESSAGE\", via: :node, payload: \"message\", removes_on: [\"DELETED\"]\nend\n";
    let specs = parse_models(&format!("{MODEL}{message}")).unwrap();
    let read = |model: &str| specs.iter().find(|spec| spec.key.as_str() == model).map(|spec| (spec.changes.iter().map(|read| (read.area.clone(), read.payload.clone())).collect::<Vec<_>>(), spec.server_revision.clone())).unwrap();
    assert_eq!(read("Chat"), (vec![("MESSAGE".into(), Some("chat".into()))], Some("serverRevision".into())));
    assert_eq!(read("Message"), (vec![("MESSAGE".into(), Some("message".into()))], None));
}
