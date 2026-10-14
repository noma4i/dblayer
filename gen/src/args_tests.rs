use crate::{emit_kotlin, emit_swift, parse_models};

fn model(body: &str) -> String {
    format!("model Sample do\n  schema do\n    string :name\n    id :ownerId\n  end\n\n{body}\nend\n")
}

fn failure(body: &str) -> (usize, String) {
    let errors = parse_models(&model(body)).unwrap_err();
    assert_eq!(errors.len(), 1, "{errors:?}");
    (errors[0].line, errors[0].message.clone())
}

#[test]
fn standard_action_args_are_typed_values_that_input_may_name() {
    let source = model("  create with: CreateSample, input: { name: :name, signedId: :signedId, momentId: :momentId }, args: { signedId: :string, momentId: :id? }\n  update with: UpdateSample, input: { sampleId: :id, signedId: :signedId }, args: { signedId: :string? }");
    let models = parse_models(&source).unwrap();
    let args: Vec<_> = models[0].actions.iter().map(|action| action.args.iter().map(|(name, kind)| format!("{name}={kind}")).collect::<Vec<_>>()).collect();
    assert_eq!(args, [vec!["momentId=id?", "signedId=string"], vec!["signedId=string?"]]);
}

#[test]
fn standard_action_args_reject_what_cannot_be_sent() {
    for (case, body, message) in [
        ("kind", "  create with: A, args: { x: :date }", "arg type must be :bool, :int, :num, :string, :id or :ids, each optionally followed by ?"),
        ("not a hash", "  create with: A, args: [1]", "`args` must be a hash like `{ field: :param }`"),
        ("field name", "  create with: A, args: { name: :string }", "arg `name` has the name of a field"),
        ("unused", "  create with: A, input: { name: :name }, args: { x: :string }", "arg `x` is not named by `input`"),
        ("destroy", "  destroy with: A, args: { x: :string }", "unknown key `args` for `destroy`"),
        ("unknown input", "  create with: A, input: { v: :ghost, w: :y }, args: { y: :string }", "input names unknown field `ghost`"),
    ] {
        assert_eq!(failure(body).1, message, "{case}");
    }
}

#[test]
fn action_args_accept_a_nullable_kind() {
    let source = model("  action :forward do\n    mutation Forward\n    local_effect :none, reason: \"test\"\n    args momentId: :id?, note: :string\n    input sampleId: :id, momentId: :momentId, note: :note\n  end");
    assert_eq!(parse_models(&source).unwrap()[0].actions[0].args["momentId"], "id?");
}

#[test]
fn the_generated_calls_take_the_args_after_the_fields() {
    let source = model("  create with: CreateSample, input: { name: :name, signedId: :signedId, momentId: :momentId }, args: { signedId: :string, momentId: :id? }\n  update with: UpdateSample, input: { sampleId: :id, signedId: :signedId }, args: { signedId: :string? }");
    let models = parse_models(&source).unwrap();
    let swift = emit_swift(&models, &[], &[]);
    for expected in [
        "create(name: String, momentId: String? = nil, signedId: String) async throws -> Self",
        "args[\"signedId\"] = JSONValue.encode(signedId)",
        "update(name: String? = nil, ownerId: String? = nil, signedId: String? = nil) async throws -> Self",
    ] {
        assert!(swift.contains(expected), "swift lacks {expected}");
    }
    let kotlin = emit_kotlin(&models, &[], &[]);
    for expected in [
        "suspend fun create(name: String, momentId: String? = null, signedId: String): Sample",
        "suspend fun update(name: String? = null, ownerId: String? = null, signedId: String? = null): Sample",
    ] {
        assert!(kotlin.contains(expected), "kotlin lacks {expected}");
    }
}

#[test]
fn a_model_without_args_emits_no_args_variable() {
    let models = parse_models(&model("  create with: CreateSample, input: { name: :name }\n  update with: UpdateSample")).unwrap();
    let swift = emit_swift(&models, &[], &[]);
    assert!(!swift.contains("var args") && !swift.contains("args: args"), "swift keeps an unused args variable");
    let kotlin = emit_kotlin(&models, &[], &[]);
    assert!(kotlin.contains("create(\"Sample\", values.toString(), \"{}\")") && kotlin.contains("update(modelName, id, values.toString(), \"{}\")"), "kotlin builds an empty args object");
}
