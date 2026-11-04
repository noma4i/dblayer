use dblayer::schema::{ModelSpec, RelationSpec};

use super::{create_fields, identifier as printed, standard_args, writable_fields, ACCOUNT, ROW_ID};
use crate::operation::OperationCall;

struct Signature {
    subject: String,
    owner: String,
    function: String,
    parameters: Vec<String>,
    fixed: Vec<&'static str>,
}

fn is_hidden_action(name: &str) -> bool {
    ["create", "update", "destroy"].contains(&name)
}

fn edge_accessor(relation: &RelationSpec) -> bool {
    !relation.edge_fields.is_empty() && relation.node.is_none()
}

fn signatures(models: &[ModelSpec], operations: &[OperationCall]) -> Vec<Signature> {
    let mut found = Vec::new();
    for model in models {
        let subject = model.key.as_str().to_string();
        let owner = format!("model `{subject}`");
        let mut push = |function: String, parameters: Vec<String>, fixed: Vec<&'static str>| found.push(Signature { subject: subject.clone(), owner: owner.clone(), function, parameters, fixed });
        let fields = writable_fields(model);
        let create: Vec<String> = create_fields(model, &fields).iter().map(|field| field.name.clone()).chain(standard_args(model, "create").iter().map(|(name, _)| name.to_string())).collect();
        push("create".into(), create, vec![ACCOUNT]);
        let update: Vec<String> = fields.iter().map(|field| field.name.clone()).chain(standard_args(model, "update").iter().map(|(name, _)| name.to_string())).collect();
        push("update".into(), update, vec![ACCOUNT]);
        if !model.ephemeral.is_empty() {
            push("set".into(), model.ephemeral.iter().map(|field| field.field.name.clone()).collect(), vec![ACCOUNT]);
        }
        for action in model.actions.iter().filter(|action| !action.upload && !is_hidden_action(&action.name)) {
            push(format!("action `{}`", action.name), action.args.keys().cloned().collect(), vec![ACCOUNT]);
        }
        for relation in model.relations.iter().filter(|relation| edge_accessor(relation)) {
            let parameters = relation.by_columns.iter().chain(&relation.text_params).cloned().collect();
            push(format!("scope `{}` edge accessor", relation.name), parameters, vec![ROW_ID, ACCOUNT]);
        }
    }
    for operation in operations {
        found.push(Signature { subject: operation.name.clone(), owner: format!("operation `{}`", operation.name), function: "call".into(), parameters: operation.args.iter().map(|(name, _)| name.clone()).collect(), fixed: vec![ACCOUNT] });
    }
    found
}

pub(crate) fn name_clashes(models: &[ModelSpec], operations: &[OperationCall]) -> Vec<(String, String)> {
    let mut found = Vec::new();
    for signature in signatures(models, operations) {
        for parameter in &signature.parameters {
            let names = [printed(parameter, true), printed(parameter, false)];
            let Some(fixed) = signature.fixed.iter().find(|fixed| names.iter().any(|name| name == *fixed)) else { continue };
            found.push((signature.subject.clone(), format!("{} {}: parameter `{parameter}` has the name of the generated parameter `{fixed}`. Rename it; the server name stays in `from:` or `variables`", signature.owner, signature.function)));
        }
    }
    found
}
