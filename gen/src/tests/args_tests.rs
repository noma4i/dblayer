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

const PAIR: &str = "operation :create do\n  mutation \"CreateSample\"\n  parameter :signedId, :string\n  parameter :momentId, :id, null: true\nend\n\noperation :update do\n  mutation \"UpdateSample\"\n  parameter :signedId, :string, null: true\n  variables sampleId: :id\nend";

#[test]
fn standard_operation_parameters_are_typed_values_that_variables_may_name() {
    let models = parse_models(&model(PAIR)).unwrap();
    let args: Vec<_> = models[0].actions.iter().map(|action| action.args.iter().map(|(name, kind)| format!("{name}={kind}")).collect::<Vec<_>>()).collect();
    assert_eq!(args, [vec!["momentId=id?", "signedId=string"], vec!["signedId=string?"]]);
}

#[test]
fn standard_operation_parameters_reject_what_cannot_be_sent() {
    for (case, body, message) in [
        ("kind", "operation :create do\n  mutation \"A\"\n  parameter :x, :date\nend", crate::decl::PARAM_TYPES),
        ("attribute name", "operation :create do\n  mutation \"A\"\n  parameter :name, :string\nend", "parameter `name` has the name of an attribute"),
        ("destroy takes no parameter", "operation :destroy do\n  mutation \"A\"\n  parameter :x, :string\nend", "`parameter` does not apply to operation `destroy`"),
        ("unknown variable source", "operation :create do\n  mutation \"A\"\n  parameter :y, :string\n  variables other: :ghost\nend", "variable `other` names unknown attribute or parameter `ghost`"),
    ] {
        let found = failure(body).1;
        assert!(found.contains(message), "{case}: {found}");
    }
}

#[test]
fn a_named_operation_accepts_a_nullable_kind() {
    let source = model("operation :forward do\n  mutation \"Forward\"\n  optimistic false\n  parameter :momentId, :id, null: true\n  parameter :note, :string\nend");
    assert_eq!(parse_models(&source).unwrap()[0].actions[0].args["momentId"], "id?");
}

#[test]
fn the_generated_calls_take_the_args_after_the_fields() {
    let models = parse_models(&model(PAIR)).unwrap();
    let swift = emit_swift(&models, &[], &SchemaTypes::default());
    for expected in [
        "create(name: String, ownerId: String, momentId: String? = nil, signedId: String, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self>",
        "args_[\"signedId\"] = try JSONValue.encode(signedId)",
        "update(name: String? = nil, ownerId: String? = nil, signedId: String? = nil, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self>",
    ] {
        assert!(swift.contains(expected), "swift lacks {expected}");
    }
    let kotlin = emit_kotlin(&models, &[], &SchemaTypes::default());
    for expected in [
        "suspend fun create(name: String, ownerId: String, momentId: String? = null, signedId: String, account: AccountScope? = null): MutationReceipt<Sample>",
        "suspend fun update(name: String? = null, ownerId: String? = null, signedId: String? = null, account: AccountScope? = null): MutationReceipt<Sample>",
    ] {
        assert!(kotlin.contains(expected), "kotlin lacks {expected}");
    }
}

#[test]
fn a_model_without_args_emits_no_args_variable() {
    let models = parse_models(&model("operation :create do\n  mutation \"CreateSample\"\nend\n\noperation :update do\n  mutation \"UpdateSample\"\nend")).unwrap();
    let swift = emit_swift(&models, &[], &SchemaTypes::default());
    assert!(!swift.contains("var args") && !swift.contains("args: args"), "swift keeps an unused args variable");
    let kotlin = emit_kotlin(&models, &[], &SchemaTypes::default());
    assert!(kotlin.contains("ordered_.create(epoch_, \"Sample\", values_.jsonText(), \"{}\")") && kotlin.contains("update(epoch_, this.modelName, this.id, values_.jsonText(), \"{}\")"), "kotlin builds an empty args object");
}

#[test]
fn gen_unknown_operation_key() {
    let found = failure("operation :create, durable: true do\n  mutation \"CreateSample\"\nend").1;
    assert!(found.contains("durable"), "{found}");
}

#[test]
fn expires_after_is_lowered_into_the_operation_spec() {
    let models = parse_models(&model("operation :create, expires_after: 2.hours do\n  mutation \"CreateSample\"\nend\n\noperation :update do\n  mutation \"UpdateSample\"\nend")).unwrap();
    let expires: Vec<_> = models[0].actions.iter().map(|action| action.expires_after).collect();
    assert_eq!(expires, [Some(7200), None]);
    assert!(failure("operation :create, expires_after: 90 do\n  mutation \"CreateSample\"\nend").1.contains("duration"));
}

#[test]
fn gen_create_maps_own_id_refused() {
    let errors = parse_models(&model("operation :create do\n  mutation \"A\"\n  variables ownerId: :id\nend")).unwrap_err();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].message.contains("create") && errors[0].message.contains("`id`"), "{}", errors[0].message);
}

/// M76: the parameter name `nodeId` belongs to the `node` read, a scope without `node:` cannot declare it.
#[test]
fn gen_node_param_name_is_reserved() {
    let scoped = |parameter: &str| format!("model Item do\n  attribute :title, :string\n  scope :node, node: :one do\n    parameter :id, :id\n    query \"ItemNode\"\n    variables id: :id\n  end\n  scope :by_owner do\n    parameter :{parameter}, :id\n    query \"ItemsByOwner\"\n    variables owner: :{parameter}\n    response from: \"items\", shape: :list\n  end\nend\n");
    let accepted = parse_models(&scoped("ownerId"));
    assert!(accepted.is_ok(), "{:?}", accepted.err());
    let errors = parse_models(&scoped("nodeId")).unwrap_err();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].message.contains("by_owner") && errors[0].message.contains("`nodeId`"), "{}", errors[0].message);
}
