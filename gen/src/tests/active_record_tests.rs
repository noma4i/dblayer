use crate::SchemaTypes;
use dblayer::store::query::{Order, Query, Where};
use dblayer::value::SortDirection;
use serde_json::json;

use crate::{emit_kotlin, emit_swift, parse_models};

#[test]
fn singular_associations_preserve_names_ending_in_s() {
    let models = parse_models("model Entry do\n  attribute :statusId, :id\n  belongs_to :status\n  has_one :address\nend\n\nmodel Status do\nend\n\nmodel Address do\n  attribute :entryId, :id\nend\n").unwrap();
    let associations = &models[0].associations;
    assert_eq!(associations[0].target.as_str(), "Status");
    assert_eq!(associations[1].target.as_str(), "Address");
}

#[test]
fn standalone_operations_match_document_names_kinds_and_inputs() {
    let directory = tempfile::tempdir().unwrap();
    let schema = directory.path().join("schema.graphql");
    let document = directory.path().join("operations.graphql");
    let source = directory.path().join("note.dbl");
    std::fs::write(&schema, "type Query { count: Int } type Mutation { save(input: NoteInput!): Int } input NoteInput { text: String!, priority: Int = 0 } input OtherInput { text: String! }").unwrap();
    std::fs::write(&document, "mutation Save($input: NoteInput!) { save(input: $input) }").unwrap();
    let mut accepted = Vec::new();
    for body in [
        "mutation \"Missing\"",
        "query Save\n parameter :input, NoteInput",
        "mutation \"Save\"\n parameter :input, OtherInput",
        "mutation \"Save\"\n parameter :input, NoteInput, null: true",
        "mutation \"Save\"",
        "mutation \"Save\"\n wrap \"input\"\n parameter :priority, :int",
        "mutation \"Save\"\n wrap \"input\"\n parameter :text, :int",
        "mutation \"Save\"\n wrap \"input\"\n parameter :text, :string\nparameter :extra, :string",
    ] {
        std::fs::write(&source, format!("operation :save do\n{body}\nend\n")).unwrap();
        if crate::compile_files(&[&source], Some(&schema), std::slice::from_ref(&document)).is_ok() { accepted.push(body); }
    }
    assert!(accepted.is_empty(), "accepted invalid operations: {accepted:?}");
    for body in ["parameter :input, NoteInput", "wrap \"input\"\n parameter :text, :string\nparameter :priority, :int, null: true", "wrap \"input\"\n parameter :text, :string"] {
        std::fs::write(&source, format!("operation :save do\n mutation \"Save\"\n{body}\nend\n")).unwrap();
        assert!(crate::compile_files(&[&source], Some(&schema), std::slice::from_ref(&document)).is_ok(), "{body}");
    }
}

#[test]
fn operation_effect_requirements_do_not_depend_on_application_model_names() {
    let accepted = ["Chat", "Group"].into_iter().filter(|model| {
        parse_models(&format!("model {model} do\n operation :leave do\n mutation \"Leave\"\n end\nend\n")).is_ok()
    }).collect::<Vec<_>>();
    assert!(accepted.is_empty(), "models exempt from declaring their effect: {accepted:?}");
}

#[test]
fn replaced_declarations_are_not_accepted_as_another_dsl() {
    let mut accepted = Vec::new();
    for body in [
        "schema do\n string :name\n end",
        "scope :all",
        "remote :all do\n query All\n end",
        "create with: Create",
        "update with: Update",
        "destroy with: Destroy",
        "scope :all, order: { name: :asc } do\n end",
        "attribute :tags, :list, of: :string",
        "attribute :meta, :object, as: Meta do\n attribute :name, :string\n end",
        "action :publish do\n mutation \"Publish\"\n optimistic false\n end",
        "operation :create, with: Create do\n end",
        "operation :publish, mutation: Publish, optimistic: { name: \"ready\" } do\n end",
        "attribute :rank, :int, index: true",
        "attribute :summary, :string, searchable: true",
        "scope :named do\n where name: :name\n end",
    ] {
        if parse_models(&format!("model Note do\n attribute :name, :string\n{body}\nend")).is_ok() { accepted.push(body); }
    }
    assert!(accepted.is_empty(), "accepted replaced declarations: {accepted:#?}");
}

#[test]
fn declarations_reject_values_that_would_be_discarded() {
    let mut accepted = Vec::new();
    for body in [
        "scope :all do\n where \"ignored\"\n end",
        "scope :all do\n order :ignored\n end",
        "scope :all, insert_on: :bogus do\n query \"All\"\n end",
        "scope :all do\n query \"All\"\n paginate size: 5, cursor: { parameter: \"after\" }\n end",
        "scope :all do\n query \"All\"\n paginate size: 5, cursor: { parameter: \"after\", from: \"a.b\", type: :bogus }\n end",
        "scope :all do\n parameter :q, :string, default: \"x\"\n query \"All\"\n end",
        "operation :create do\n  mutation \"Create\"\n  optimistic false\nend",
        "operation :create do\nend",
        "operation :act do\n  mutation \"Act\"\n  optimistic false\n  item name: :name\nend",
        "operation :act do\n  mutation \"Act\"\n  optimistic false\n  once\nend",
        "operation :act do\n  mutation \"Act\"\n  optimistic false, reason: \"ignored\"\nend",
        "search :name, ignored: true",
    ] {
        let source = format!("model Note do\n attribute :name, :string\n{body}\nend\n");
        if parse_models(&source).is_ok() { accepted.push(body); }
    }
    assert!(accepted.is_empty(), "accepted ignored syntax: {accepted:#?}");
}

#[test]
fn filtered_ordered_query_uses_one_index_without_a_temporary_sort() {
    for direction in [SortDirection::Asc, SortDirection::Desc, SortDirection::AscNullsLast, SortDirection::DescNullsFirst] {
    let models = parse_models(&format!("model Note, storage: :local do\n attribute :folderId, :id\n attribute :position, :int, null: true\n scope :in_folder do\n parameter :folderId, :id\n where folderId: :folderId\n order position: :{}\n end\nend\n", direction.name())).unwrap();
    let db = rusqlite::Connection::open_in_memory().unwrap();
    db.execute_batch(&dblayer::store::ddl::create_table_statement(&models[0])).unwrap();
    for sql in dblayer::store::ddl::create_index_statements(&models[0]) { db.execute_batch(&sql).unwrap(); }
    let query = Query::new("Note").filtered(Where::equal("folderId", json!("folder"))).ordered(vec![Order::new("position", direction)]);
    let statement = dblayer::store::query::compile_keys(&query, &models[0]).unwrap();
    let mut explain = db.prepare(&format!("EXPLAIN QUERY PLAN {}", statement.sql)).unwrap();
    let plan = explain.query_map(rusqlite::params_from_iter(statement.bindings), |row| row.get::<_, String>(3)).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    assert!(plan.iter().any(|line| line.to_lowercase().contains("using index row_note_idx_")), "{plan:?}");
    assert!(!plan.iter().any(|line| line.contains("TEMP B-TREE")), "{plan:?}");
    }
}

#[test]
fn schema_fields_are_inferred_without_losing_local_fields() {
    let directory = tempfile::tempdir().unwrap();
    let schema = directory.path().join("schema.graphql");
    let source = directory.path().join("note.dbl");
    std::fs::write(&schema, "type Query { note: Note }\ntype Note { id: ID! body: String! score: Int }").unwrap();
    std::fs::write(&source, "model Note do\n  attribute :body\n  attribute :score\n  attribute :draft, :string, local: true, null: true\nend\n").unwrap();
    let checked = crate::compile_files(&[source], Some(&schema), &[]).unwrap();
    let swift = emit_swift(&checked.models, &checked.operations, &checked.inputs);
    let kotlin = emit_kotlin(&checked.models, &checked.operations, &checked.inputs);
    for part in ["let body: String", "let score: Int?", "let draft: String?"] { assert!(swift.contains(part), "{swift}"); }
    for part in ["val body: String", "val score: Long?", "val draft: String?"] { assert!(kotlin.contains(part), "{kotlin}"); }
}

#[test]
fn explicit_fields_do_not_require_a_graphql_type_named_after_the_model() {
    let directory = tempfile::tempdir().unwrap();
    let schema = directory.path().join("schema.graphql");
    let source = directory.path().join("note.dbl");
    std::fs::write(&schema, "type Query { count: Int }").unwrap();
    std::fs::write(&source, "model Note do\n  attribute :meta, Meta do\n    attribute :title, :string\n  end\nend\n").unwrap();
    assert!(crate::compile_files(&[source], Some(&schema), &[]).is_ok());
}

#[test]
fn local_models_open_without_graphql_and_generate_both_languages() {
    let models = parse_models(r#"model Note, storage: :local do
  attribute :text, :string
  attribute :folderId, :id
  attribute :position, :int
  search :text
  index :position
  scope :in_folder do
    parameter :folderId, :id
    where folderId: :folderId
    order position: :desc
  end
end
"#).unwrap();
    let swift = emit_swift(&models, &[], &SchemaTypes::default());
    let kotlin = emit_kotlin(&models, &[], &SchemaTypes::default());
    assert!(swift.contains("func inFolder(") && swift.contains("func create("), "{swift}");
    assert!(kotlin.contains("fun inFolder(") && kotlin.contains("fun create("), "{kotlin}");
    dblayer::store::Store::open_models(None, models, std::sync::Arc::new(dblayer::runtime::SystemClock)).unwrap();
}

/// B4 (old line emit/mod.rs creates_locally): a model whose create the server answers gets the typed `create` in both languages, so the app never writes the generic call.
#[test]
fn a_create_the_server_answers_generates_a_typed_create_in_both_languages() {
    let models = parse_models("model Club do\n  attribute :name, :string\n  operation :create do\n    mutation \"CreateClub\"\n    variables name: :name\n  end\nend\n").unwrap();
    let swift = emit_swift(&models, &[], &SchemaTypes::default());
    let kotlin = emit_kotlin(&models, &[], &SchemaTypes::default());
    assert!(swift.contains("static func create(name: String") && !swift.contains("static func send("), "{swift}");
    assert!(kotlin.contains("fun create(name: String") && !kotlin.contains("fun send("), "{kotlin}");
}
