use std::fs;

use dblayer::schema::{PreloadChoice, Storage};

use super::default_decisions_tests::{refused, refused_at};
use crate::{compile_files, emit_kotlin, emit_swift, parse_all, parse_models, Declarations, FileError};

const NOTE_SCHEMA: &str = "type Query { notes: [Note!]! }\ntype Note { id: ID! text: String! score: Int title: String }";
const NOTE_OPERATIONS: &str = "query Notes { notes { id text score title } }";

fn compile_note(model: &str) -> Result<Declarations, Vec<FileError>> {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("note.dbl");
    let schema = directory.path().join("schema.graphql");
    let queries = directory.path().join("queries");
    fs::write(&file, model).unwrap();
    fs::write(&schema, NOTE_SCHEMA).unwrap();
    fs::create_dir(&queries).unwrap();
    fs::write(queries.join("notes.graphql"), NOTE_OPERATIONS).unwrap();
    compile_files(&[file], Some(&schema), &[queries])
}

fn note_model(attributes: &str) -> String {
    format!("model Note do\n{attributes}  scope :all do\n    query \"Notes\"\n    response shape: :list\n  end\nend\n")
}

#[test]
fn gen_names_one_rule() {
    assert_eq!(crate::naming::camel("post-send_id"), "PostSendId");
    assert_eq!(crate::naming::member("post-send_id"), "postSendId");
    let source = "model Sample do\n  attribute :media_info, :json, null: true\n  attribute :detail, SampleDetail, null: true do\n    attribute :id, :id\n  end\n  scope :recent_posts do\n    query \"RecentPosts\"\n  end\n  operation :send_note do\n    mutation \"SendNote\"\n    optimistic false\n    returns do\n      attribute :id, :string\n    end\n  end\nend\n";
    let declared = parse_all(&[source]).unwrap();
    let inputs = Default::default();
    for output in [emit_swift(&declared.models, &declared.operations, &inputs), emit_kotlin(&declared.models, &declared.operations, &inputs)] {
        assert!(output.contains("recentPosts") && output.contains("sendNote") && output.contains("SampleSendNoteResult"), "{output}");
        assert!(!output.contains("RecentPosts") || output.contains("\"RecentPosts\""), "{output}");
    }
}

#[test]
fn gen_from_defaults_to_name() {
    let refusal = compile_note(&note_model("  attribute :body, :string\n")).unwrap_err().remove(0);
    assert!(refusal.file.ends_with("note.dbl") && refusal.error.line == 2 && refusal.error.message.contains("body"), "{refusal}");
    let checked = compile_note(&note_model("  attribute :body, :string, from: \"text\"\n")).unwrap();
    assert_eq!(checked.models[0].field("body").unwrap().from.as_deref(), Some("text"));
    assert!(compile_note(&note_model("  attribute :text, :string\n")).is_ok());
}

#[test]
fn gen_parameter_column_default() {
    let models = parse_models("model Sample do\n  attribute :ownerId, :id\n  scope :by_owner do\n    parameter :owner, :id\n    where ownerId: :owner\n    query \"S\"\n    preload owner: [\"1\"]\n  end\n  scope :by_channel do\n    parameter :channel, :string\n    query \"T\"\n    preload channel: [\"a\"]\n  end\nend\n").unwrap();
    let preload = |name: &str| models[0].relations.iter().find(|relation| relation.name == name).unwrap().preload.clone().unwrap();
    assert_eq!(preload("by_owner").keys().collect::<Vec<_>>(), ["ownerId"]);
    assert_eq!(preload("by_channel").keys().collect::<Vec<_>>(), ["channel"]);
    assert!(matches!(preload("by_channel")["channel"], PreloadChoice::Listed(_)));
    assert!(crate::check::check_source("model Sample do\n  scope :by_channel do\n    parameter :channel, :string\n    query \"T\"\n    preload channel: [\"a\"]\n  end\nend\n").is_ok());
}

#[test]
fn gen_preload_source_named() {
    let scopes = |source: &str| format!("model Sample do\n  scope :feed do\n    query \"F\"\n  end\n  scope :by_vibe do\n    parameter :vibeId, :id\n    query \"S\"\n    preload vibeId: {{ from: \"{source}\" }}, limit: 40\n  end\nend\n");
    assert!(parse_models(&scopes("Sample.feed")).is_ok());
    refused_at(&scopes("Sample"), 8, "source scope");
}

#[test]
fn gen_storage_default_disk() {
    let storages: Vec<_> = ["model A do\nend\n", "model B, storage: :memory do\nend\n", "model C, storage: :local do\nend\n"].iter().map(|source| parse_models(source).unwrap()[0].storage).collect();
    assert_eq!(storages, [Storage::Disk, Storage::Memory, Storage::Local]);
}

#[test]
fn gen_nullability_explicit_then_schema() {
    let checked = compile_note(&note_model("  attribute :text, :string\n  attribute :score, :int\n  attribute :title, :string\n")).unwrap();
    let nullable: Vec<_> = ["text", "score", "title"].iter().map(|name| checked.models[0].field(name).unwrap().nullable).collect();
    assert_eq!(nullable, [false, true, true]);
    let explicit = compile_note(&note_model("  attribute :text, :string, null: true\n")).unwrap();
    assert!(explicit.models[0].field("text").unwrap().nullable);
    let compiled = |declared: &str, variable: &str| {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("note.dbl");
        let schema = directory.path().join("schema.graphql");
        let queries = directory.path().join("queries");
        fs::write(&file, format!("model Note do\n  attribute :text, :string\n  scope :all do\n    parameter :kind, :string{declared}\n    query \"Notes\"\n    response shape: :list\n  end\nend\n")).unwrap();
        fs::write(&schema, "type Query { notes(kind: String): [Note!]! }\ntype Note { id: ID! text: String! }").unwrap();
        fs::create_dir(&queries).unwrap();
        fs::write(queries.join("notes.graphql"), format!("query Notes($kind: {variable}) {{ notes(kind: $kind) {{ id text }} }}")).unwrap();
        compile_files(&[file], Some(&schema), &[queries])
    };
    let mismatch = compiled(", null: true", "String!").unwrap_err().remove(0);
    assert!(mismatch.file.ends_with("note.dbl") && mismatch.error.line >= 1 && mismatch.error.message.contains("kind"), "{mismatch}");
    assert!(compiled("", "String!").is_ok() && compiled("", "String").is_ok() && compiled(", null: true", "String").is_ok());
}

#[test]
fn gen_input_wrap_default() {
    let source = |wrap: &str| format!("model Sample do\n  operation :poke do\n    mutation \"PokeSample\"\n    {wrap}\n    parameter :title, :string\n    optimistic false\n  end\nend\n");
    let print = |source: &str| {
        let declared = parse_all(&[source]).unwrap();
        (emit_swift(&declared.models, &declared.operations, &Default::default()), emit_kotlin(&declared.models, &declared.operations, &Default::default()))
    };
    let plain = parse_models(&source("")).unwrap();
    assert!(plain[0].actions[0].wrap.is_none());
    let wrapped = parse_models(&source("wrap \"input\"")).unwrap();
    assert_eq!(wrapped[0].actions[0].wrap.as_deref(), Some("input"));
    let standalone = "operation :poke_send do\n  mutation \"PokeSend\"\n  parameter :title, :string\nend\noperation :poke_wrapped do\n  mutation \"PokeWrapped\"\n  parameter :title, :string\nend\n";
    let (swift, kotlin) = print(standalone);
    assert!(swift.contains("variables: variables_,") && swift.contains("variables: [\"input\": JSONValue.object(variables_)],"), "{swift}");
    assert!(kotlin.contains("JsonObject(variables_)") && kotlin.contains("mapOf(\"input\" to JsonObject(variables_))"), "{kotlin}");
}

#[test]
fn gen_case_affix_rule() {
    let union = |types: &[(&str, &str)]| {
        let blocks: String = types.iter().map(|(name, option)| format!("    on {name}{option} do\n      attribute :id, :id\n    end\n")).collect();
        format!("model Item do\n  attribute :props, ItemProps, null: true do\n{blocks}  end\nend\n")
    };
    let cases = |source: &str| parse_models(source).unwrap()[0].field("props").unwrap().fields.iter().map(|variant| variant.name.clone()).collect::<Vec<_>>();
    assert_eq!(cases(&union(&[("GiftAttachment", ""), ("MomentAttachment", "")])), ["gift", "moment"]);
    assert_eq!(cases(&union(&[("GiftAttachment", ""), ("Gift", "")])), ["giftAttachment", "gift"]);
    assert_eq!(cases(&union(&[("MomentEmojiProps", ""), ("MomentGifProps", ""), ("MomentHashtagProps", "")])), ["emoji", "gif", "hashtag"]);
    let clash = refused(&union(&[("FirstProps", ", case: :second"), ("SecondProps", "")]));
    assert!(clash.error.message.contains("second") && clash.error.message.contains("case:"), "{clash}");
}

#[test]
fn gen_schema_lookup_strict() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("note.dbl");
    let schema = directory.path().join("schema.graphql");
    fs::write(&file, note_model("  attribute :text, :string\n")).unwrap();
    fs::write(&schema, NOTE_SCHEMA).unwrap();
    let missing = directory.path().join("absent.graphql");
    let unreadable = compile_files(&[file.clone()], Some(&schema), &[missing.clone()]).unwrap_err().remove(0);
    assert_eq!(unreadable.file, missing.display().to_string());
    assert!(unreadable.error.line >= 1, "{unreadable}");
    let queries = directory.path().join("queries");
    fs::create_dir(&queries).unwrap();
    fs::write(queries.join("notes.graphql"), NOTE_OPERATIONS).unwrap();
    let source = "model Note do\n  attribute :text, :string\n  scope :all do\n    query \"Notes\"\n    response from: \"notes.ghost\", shape: :list\n  end\nend\n";
    fs::write(&file, source).unwrap();
    let refusal = compile_files(&[file.clone()], Some(&schema), &[queries.clone()]).unwrap_err().remove(0);
    assert!(refusal.file.ends_with("note.dbl") && refusal.error.line == 4 && refusal.error.message.contains("ghost"), "{refusal}");
    fs::write(&file, note_model("  attribute :text, :string\n")).unwrap();
    assert!(compile_files(&[file], Some(&schema), &[queries]).is_ok());
}

#[test]
fn gen_error_positions_exact() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("note.dbl");
    let schema = directory.path().join("schema.graphql");
    let queries = directory.path().join("queries");
    fs::create_dir(&queries).unwrap();
    fs::write(&file, note_model("  attribute :text, :string\n")).unwrap();
    fs::write(&schema, NOTE_SCHEMA).unwrap();
    fs::write(queries.join("notes.graphql"), "query Notes { notes { ...Missing } }").unwrap();
    let broken_schema = directory.path().join("broken.graphql");
    fs::write(&broken_schema, "type Query {").unwrap();
    let huge = format!("model Sample do\n  attribute :ratio, :num, default: {}.5\nend\n", "9".repeat(400));
    let huge_file = directory.path().join("huge.dbl");
    fs::write(&huge_file, huge).unwrap();
    let runs: Vec<Vec<FileError>> = vec![
        compile_files(&[directory.path().join("absent.dbl")], None, &[]).unwrap_err(),
        compile_files(&[file.clone()], Some(&broken_schema), &[]).unwrap_err(),
        compile_files(&[file.clone()], Some(&schema), &[queries]).unwrap_err(),
        compile_files(&[huge_file], None, &[]).unwrap_err(),
    ];
    for errors in runs {
        for error in errors {
            assert!(!error.file.is_empty() && error.error.line >= 1 && error.error.column >= 1, "{error:?}");
        }
    }
    let infinite = compile_files(&[directory.path().join("huge.dbl")], None, &[]).unwrap_err().remove(0);
    assert_eq!((infinite.error.line, infinite.error.message.contains("finite")), (2, true), "{infinite}");
}

#[test]
fn gen_operation_without_returns_raw() {
    let declared = parse_all(&["operation :sign_out do\n  mutation \"SignOut\"\nend\n"]).unwrap();
    assert!(declared.operations[0].result.is_none());
    let swift = emit_swift(&declared.models, &declared.operations, &Default::default());
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &Default::default());
    assert!(swift.contains("try await DB.call(\"SignOut\", variables: variables_, in: scope_)") && !swift.contains("SignOutResult"), "{swift}");
    assert!(kotlin.contains("DBlayer.call(\"SignOut\", JsonObject(variables_), account)") && !kotlin.contains("SignOutResult"), "{kotlin}");
}

#[test]
fn gen_refetch_own_model() {
    let source = "model Sample do\n  scope :feed do\n    query \"F\"\n  end\n  operation :poke do\n    mutation \"Poke\"\n    optimistic false\n    refetch :feed\n  end\nend\n\nmodel Other do\n  scope :feed do\n    query \"G\"\n  end\n  operation :nudge do\n    mutation \"Nudge\"\n    optimistic false\n    refetch :feed, model: Sample\n  end\nend\n";
    let models = crate::check::check_source(source).unwrap();
    assert_eq!((models[0].actions[0].refetch.clone(), models[1].actions[0].refetch.clone()), (vec!["feed".to_string()], vec!["Sample.feed".to_string()]));
}

#[test]
fn gen_association_key_not_in_schema() {
    let model = "model Note do\n  attribute :text, :string\n  attribute :ownerId, :id\n  index :ownerId\n  belongs_to :owner, class_name: Note, foreign_key: :ownerId\n  scope :all do\n    query \"Notes\"\n    response shape: :list\n  end\nend\n";
    let checked = compile_note(model);
    assert!(checked.is_ok(), "{:?}", checked.err());
}
