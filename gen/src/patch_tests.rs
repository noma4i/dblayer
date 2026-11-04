use crate::SchemaTypes;
use crate::{emit_kotlin, emit_swift, parse_all};

const NOTE: &str = "model Note do\n  attribute :title, :string\n  attribute :body, :string, null: true\n  ephemeral do\n    attribute :typing, :bool, default: false\n    attribute :mood, :string, null: true\n  end\nend\n";

#[test]
fn a_nullable_field_has_three_states_in_update_and_set_on_both_platforms() {
    let declared = parse_all(&[NOTE]).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &SchemaTypes::default());
    for line in ["@concurrent public func update(title: String? = nil, body: String?? = nil, in scope_: AccountScope? = nil)", "@concurrent public func set(typing: Bool? = nil, mood: String?? = nil, in scope_: AccountScope? = nil)"] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &SchemaTypes::default());
    for line in [
        "suspend fun update(title: String? = null, body: GraphQLNullable<String> = GraphQLNullable.Omitted, account_: AccountScope? = null)",
        "title?.let { put(\"title\", jsonValue(it)) }",
        "put(\"body\", body)",
        "suspend fun set(typing: Boolean? = null, mood: GraphQLNullable<String> = GraphQLNullable.Omitted, account_: AccountScope? = null)",
        "put(\"mood\", mood)",
    ] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
}

#[test]
fn an_integer_in_a_json_default_stays_an_integer_in_swift() {
    let declared = parse_all(&["model Prefs do\n  attribute :props, :json, default: { size: 3 }\nend\n"]).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &SchemaTypes::default());
    assert!(swift.contains("\"default\":{\"size\":3}"), "{swift}");
}
