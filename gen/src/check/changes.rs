use super::*;

const AREAS: [&str; 9] = ["CHAT", "MESSAGE", "MOMENT", "REACTION", "GIFT", "WALLET", "COUNTERS", "USER", "CUSTOM_SCREEN"];

pub(super) fn check_changes(nodes: &[ModelNode], models: &[ModelSpec], errors: &mut Vec<Found>) {
    let mut seen: std::collections::HashMap<&str, (bool, usize, usize, usize)> = std::collections::HashMap::new();
    for (node, model) in nodes.iter().zip(models) {
        for (call, change) in node.body.iter().filter(|call| call.word == "changed_by").zip(&model.changes) {
            let mut report = |message: String| errors.push(at(node.file, call.line, call.column, message));
            if !AREAS.contains(&change.area.as_str()) { report(format!("unknown change area `{}`", change.area)); }
            let Some(relation) = model.relations.iter().find(|relation| relation.name == change.relation) else {
                report(format!("unknown relation `{}`", change.relation));
                continue;
            };
            if relation.remote.is_none() { report(format!("relation `{}` has no remote read", change.relation)); }
            let node_read = relation.node == Some(dblayer::schema::NodeRead::One);
            let parameters: Vec<&str> = relation.text_params.iter().chain(&relation.by_columns).map(String::as_str).collect();
            match (&change.parameter, node_read) {
                (Some(_), true) => report("node read takes its id parameter from entityId automatically".into()),
                (Some(parameter), false) if parameters.len() != 1 || parameters[0] != parameter => report(format!("unknown or ambiguous parameter `{parameter}`")),
                (None, false) if !parameters.is_empty() => report("a parameter is required for this relation".into()),
                _ => {}
            }
            if let Some((previous_node, file, line, column)) = seen.get(change.area.as_str()) {
                if !node_read || !previous_node {
                    report(format!("duplicate change area `{}`, first declaration at file {file}:{line}:{column}", change.area));
                }
            } else {
                seen.insert(&change.area, (node_read, node.file, call.line, call.column));
            }
        }
    }
}
