use crate::SchemaTypes;
use crate::{emit_kotlin, emit_swift, parse_models};

fn model(body: &str) -> String {
    format!("model Sample do\n\n    attribute :name, :string\n    attribute :ownerId, :id\n\n\n{body}\nend\n")
}

fn failure(body: &str) -> (usize, String) {
    let errors = parse_models(&model(body)).unwrap_err();
    assert_eq!(errors.len(), 1, "{errors:?}");
    (errors[0].line, errors[0].message.clone())
}

const PAIR: &str = "operation :create, durable: true do\n  mutation \"CreateSample\"\n  parameter :signedId, :string\n  parameter :momentId, :id, null: true\n  variables name: :name, signedId: :signedId, momentId: :momentId\nend\n\noperation :update do\n  mutation \"UpdateSample\"\n  parameter :signedId, :string, null: true\n  variables sampleId: :id, signedId: :signedId\nend";

#[test]
fn standard_operation_parameters_are_typed_values_that_variables_may_name() {
    let models = parse_models(&model(PAIR)).unwrap();
    let args: Vec<_> = models[0].actions.iter().map(|action| action.args.iter().map(|(name, kind)| format!("{name}={kind}")).collect::<Vec<_>>()).collect();
    assert_eq!(args, [vec!["momentId=id?", "signedId=string"], vec!["signedId=string?"]]);
}

#[test]
fn standard_operation_parameters_reject_what_cannot_be_sent() {
    for (case, body, message) in [
        ("kind", "operation :create do\n  mutation \"A\"\n  parameter :x, :date\n  variables x: :x\nend", crate::decl::PARAM_TYPES),
        ("attribute name", "operation :create do\n  mutation \"A\"\n  parameter :name, :string\n  variables name: :name\nend", "parameter `name` has the name of an attribute"),
        ("unused", "operation :create do\n  mutation \"A\"\n  parameter :x, :string\nend", "parameter `x` is used by neither `variables` nor `optimistic`"),
        ("destroy takes no parameter", "operation :destroy do\n  mutation \"A\"\n  parameter :x, :string\nend", "`parameter` does not apply to operation `destroy`"),
        ("unknown variable source", "operation :create do\n  mutation \"A\"\n  parameter :y, :string\n  variables y: :y, ghost: :ghost\nend", "variable `ghost` names unknown attribute or parameter `ghost`"),
    ] {
        let found = failure(body).1;
        assert!(found.contains(message), "{case}: {found}");
    }
}

#[test]
fn a_named_operation_accepts_a_nullable_kind() {
    let source = model("operation :forward do\n  mutation \"Forward\"\n  optimistic false\n  parameter :momentId, :id, null: true\n  parameter :note, :string\n  variables momentId: :momentId, note: :note\nend");
    assert_eq!(parse_models(&source).unwrap()[0].actions[0].args["momentId"], "id?");
}

#[test]
fn the_generated_calls_take_the_args_after_the_fields() {
    let models = parse_models(&model(PAIR)).unwrap();
    let swift = emit_swift(&models, &[], &SchemaTypes::default());
    for expected in [
        "send(in scope: AccountScope, name: String, momentId: String? = nil, signedId: String) async throws -> Sent<Self>",
        "args[\"signedId\"] = try JSONValue.encode(signedId)",
        "update(name: String? = nil, ownerId: String? = nil, signedId: String? = nil, in scope: AccountScope? = nil) async throws -> Self",
    ] {
        assert!(swift.contains(expected), "swift lacks {expected}");
    }
    let kotlin = emit_kotlin(&models, &[], &SchemaTypes::default());
    for expected in [
        "suspend fun send(account: AccountScope, name: String, momentId: String? = null, signedId: String): Sent<Sample>",
        "suspend fun update(name: String? = null, ownerId: String? = null, signedId: String? = null, account: AccountScope? = null): Sample",
    ] {
        assert!(kotlin.contains(expected), "kotlin lacks {expected}");
    }
}

#[test]
fn a_model_without_args_emits_no_args_variable() {
    let models = parse_models(&model("operation :create, durable: true do\n  mutation \"CreateSample\"\nend\n\noperation :update do\n  mutation \"UpdateSample\"\nend")).unwrap();
    let swift = emit_swift(&models, &[], &SchemaTypes::default());
    assert!(!swift.contains("var args") && !swift.contains("args: args"), "swift keeps an unused args variable");
    let kotlin = emit_kotlin(&models, &[], &SchemaTypes::default());
    assert!(kotlin.contains("account.send(this, values)") && kotlin.contains("update(admitted(account), modelName, id, values.jsonText(), \"{}\")"), "kotlin builds an empty args object");
}
