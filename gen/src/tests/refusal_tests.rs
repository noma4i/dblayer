use std::fs;

use super::default_decisions_tests::{refused, refused_at};
use crate::{compile_files, emit_kotlin, emit_swift, load_schema_types, parse_all, specs_text};

const SCHEMA: &str = "type Query { a: Int }\nenum Mood { HAPPY SAD }\ninput PostInput { title: String!, note: String, count: Int! = 3, mood: Mood }\ninput PickInput @oneOf { id: ID, name: String }\n";
const SEND: &str = "operation :post_send do\n  mutation \"PostSend\"\n  parameter :input, PostInput\n  parameter :pick, PickInput\nend\n";

fn outputs() -> (String, String) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("schema.graphql");
    fs::write(&path, SCHEMA).unwrap();
    let declared = parse_all(&[SEND]).unwrap();
    let inputs = load_schema_types(Some(&path), &declared.scalars, &declared.models, &declared.operations).unwrap();
    (emit_swift(&declared.models, &declared.operations, &inputs), emit_kotlin(&declared.models, &declared.operations, &inputs))
}

#[test]
fn gen_operation_variable_domain() {
    refused_at("model Sample do\n  operation :like do\n    mutation \"Like\"\n    variables text: \"literal\"\n  end\nend\n", 4, "variable");
    let accepted = "model Sample do\n  scope :feed do\n    query \"Feed\"\n    parameter :status, :string\n    variables kind: \"image\", status: :status\n  end\nend\n";
    assert!(parse_all(&[accepted]).is_ok());
}

#[test]
fn gen_input_absence_parity() {
    let (swift, kotlin) = outputs();
    assert!(swift.contains("public let note: GraphQLNullable<String>") && swift.contains("try container.encodeIfPresent(self.count, forKey: .count)"), "{swift}");
    assert!(kotlin.contains("@EncodeDefault(EncodeDefault.Mode.NEVER) val note: GraphQLNullable<String> = GraphQLNullable.Omitted") && kotlin.contains("@EncodeDefault(EncodeDefault.Mode.NEVER) val count: Long? = null"), "{kotlin}");
}

#[test]
fn gen_input_oneof_parity() {
    let (swift, kotlin) = outputs();
    assert!(swift.contains("case id(String)") && swift.contains("case name(String)") && swift.contains("case .name(let value): try container.encode(value, forKey: .name)"), "{swift}");
    assert!(kotlin.contains("data class Id(val value: String) : PickInput") && kotlin.contains("is PickInput.Name -> \"name\" to jsonValue(value.value)"), "{kotlin}");
}

#[test]
fn gen_returns_policy() {
    for option in ["null: true", "optional: true", "default: \"a\"", "from: \"b\"", "local: true"] {
        let error = refused(&format!("operation :send do\n  mutation \"Send\"\n  returns do\n    attribute :id, :string, {option}\n  end\nend\n"));
        assert!(error.file.ends_with("m.dbl") && error.error.line == 4, "{option}: {error}");
    }
    assert!(parse_all(&["operation :send do\n  mutation \"Send\"\n  returns do\n    attribute :id, :string\n  end\nend\n"]).is_ok());
}

#[test]
fn gen_unknown_tag_parity() {
    let source = "model Item do\n  attribute :kind, :enum, values: [\"A\", \"B\"]\n  attribute :props, ItemProps, null: true do\n    on FirstProps do\n      attribute :id, :id\n    end\n    on SecondProps do\n      attribute :id, :id\n    end\n  end\nend\n";
    let declared = parse_all(&[source]).unwrap();
    let inputs = Default::default();
    let swift = emit_swift(&declared.models, &declared.operations, &inputs);
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &inputs);
    assert!(swift.contains("case unknown(String)") && swift.contains("case unknown(typename: String)"), "{swift}");
    assert!(kotlin.contains("class Unknown(") , "{kotlin}");
    let clash = "model Item do\n  attribute :props, ItemProps do\n    on FirstProps, case: :same do\n      attribute :id, :id\n    end\n    on SecondProps, case: :same do\n      attribute :id, :id\n    end\n  end\nend\n";
    refused_at(clash, 2, "same");
}

#[test]
fn gen_error_not_dropped() {
    refused_at("model Sample do\n  attribute :name, :string\nend\n\nmodel Sample do\n  attribute :name, :string\nend\n", 5, "twice");
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("bad.dbl");
    fs::write(&file, "model Sample do\n  bogus\nend\n").unwrap();
    let errors = compile_files(&[file], None, &[]).unwrap_err();
    assert!(errors.iter().any(|error| error.file.ends_with("bad.dbl") && error.error.line == 2), "{errors:?}");
}

#[test]
fn gen_declaration_orders() {
    let owner = "model Owner do\n  attribute :name, :string\n  has_many :pets\nend\n";
    let pet = "model Pet do\n  attribute :ownerId, :id\n  attribute :age, :int\n  scope :young do\n    order age: :asc\n  end\nend\n";
    let build = |sources: [&str; 2]| {
        let directory = tempfile::tempdir().unwrap();
        let paths: Vec<_> = sources.iter().enumerate().map(|(at, text)| { let path = directory.path().join(format!("{at}.dbl")); fs::write(&path, text).unwrap(); path }).collect();
        let mut declared = compile_files(&paths, None, &[]).unwrap();
        declared.models.sort_by(|a, b| a.key.as_str().cmp(b.key.as_str()));
        (specs_text(&declared.models, &declared.operations), emit_swift(&declared.models, &declared.operations, &declared.inputs), emit_kotlin(&declared.models, &declared.operations, &declared.inputs))
    };
    assert_eq!(build([owner, pet]), build([pet, owner]));
}
