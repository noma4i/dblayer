use crate::SchemaTypes;
use crate::{emit_kotlin, emit_swift, parse_models};

fn model(body: &str) -> String {
    format!("model Sample do\n\n    field :name, type: :string\n    field :ownerId, type: :id\n\n\n{body}\nend\n")
}

fn failure(body: &str) -> (usize, String) {
    let errors = parse_models(&model(body)).unwrap_err();
    assert_eq!(errors.len(), 1, "{errors:?}");
    (errors[0].line, errors[0].message.clone())
}

#[test]
fn standard_action_args_are_typed_values_that_input_may_name() {
    let source = model("  operation :create do\n    mutation CreateSample\n    input name: :name, signedId: :signedId, momentId: :momentId\n    args signedId: :string, momentId: :id?\n  end\n  operation :update do\n    mutation UpdateSample\n    input sampleId: :id, signedId: :signedId\n    args signedId: :string?\n  end");
    let models = parse_models(&source).unwrap();
    let args: Vec<_> = models[0].actions.iter().map(|action| action.args.iter().map(|(name, kind)| format!("{name}={kind}")).collect::<Vec<_>>()).collect();
    assert_eq!(args, [vec!["momentId=id?", "signedId=string"], vec!["signedId=string?"]]);
}

#[test]
fn standard_action_args_reject_what_cannot_be_sent() {
    for (case, body, message) in [
        ("kind", "  operation :create do\n    mutation A\n    args x: :date\n  end", crate::operation::ARG_KIND_MESSAGE),
        ("positional value", "  operation :create do\n    mutation A\n    args 1\n  end", "`args` needs named values"),
        ("field name", "  operation :create do\n    mutation A\n    args name: :string\n  end", "arg `name` has the name of a field"),
        ("unused", "  operation :create do\n    mutation A\n    input name: :name\n    args x: :string\n  end", "arg `x` is not named by `input`"),
        ("destroy", "  operation :destroy do\n    mutation A\n    args x: :string\n  end", "unknown key `args` for `destroy`"),
        ("unknown input", "  operation :create do\n    mutation A\n    input v: :ghost, w: :y\n    args y: :string\n  end", "input names unknown field `ghost`"),
    ] {
        assert_eq!(failure(body).1, message, "{case}");
    }
}

#[test]
fn action_args_accept_a_nullable_kind() {
    let source = model("  operation :forward do\n    mutation Forward\n    local_effect :none, reason: \"test\"\n    args momentId: :id?, note: :string\n    input sampleId: :id, momentId: :momentId, note: :note\n  end");
    assert_eq!(parse_models(&source).unwrap()[0].actions[0].args["momentId"], "id?");
}

#[test]
fn the_generated_calls_take_the_args_after_the_fields() {
    let source = model("  operation :create do\n    mutation CreateSample\n    input name: :name, signedId: :signedId, momentId: :momentId\n    args signedId: :string, momentId: :id?\n  end\n  operation :update do\n    mutation UpdateSample\n    input sampleId: :id, signedId: :signedId\n    args signedId: :string?\n  end");
    let models = parse_models(&source).unwrap();
    let swift = emit_swift(&models, &[], &SchemaTypes::default());
    for expected in [
        "create(name: String, momentId: String? = nil, signedId: String) async throws -> Self",
        "args[\"signedId\"] = JSONValue.encode(signedId)",
        "update(name: String? = nil, ownerId: String? = nil, signedId: String? = nil) async throws -> Self",
    ] {
        assert!(swift.contains(expected), "swift lacks {expected}");
    }
    let kotlin = emit_kotlin(&models, &[], &SchemaTypes::default());
    for expected in [
        "suspend fun create(name: String, momentId: String? = null, signedId: String): Sample",
        "suspend fun update(name: String? = null, ownerId: String? = null, signedId: String? = null): Sample",
    ] {
        assert!(kotlin.contains(expected), "kotlin lacks {expected}");
    }
}

#[test]
fn a_model_without_args_emits_no_args_variable() {
    let models = parse_models(&model("  operation :create do\n    mutation CreateSample\n    input name: :name\n  end\n  operation :update do\n    mutation UpdateSample\n  end")).unwrap();
    let swift = emit_swift(&models, &[], &SchemaTypes::default());
    assert!(!swift.contains("var args") && !swift.contains("args: args"), "swift keeps an unused args variable");
    let kotlin = emit_kotlin(&models, &[], &SchemaTypes::default());
    assert!(kotlin.contains("create(\"Sample\", values.toString(), \"{}\")") && kotlin.contains("update(modelName, id, values.toString(), \"{}\")"), "kotlin builds an empty args object");
}
