use dblayer::schema::{ModelSpec, NodeRead};

use super::{at, Found};
use crate::decl::ModelDecl;

pub(super) fn check_changes(nodes: &[ModelDecl], models: &[ModelSpec], errors: &mut Vec<Found>) {
    let feeds: Vec<_> = nodes.iter().flat_map(|node| node.changes.iter().map(move |feed| (node, feed))).collect();
    for (node, feed) in feeds.iter().skip(1) {
        errors.push(at(node.file, feed.pos, "duplicate changes subscription"));
    }
    let mut seen: std::collections::HashMap<&str, (bool, usize, usize, usize)> = std::collections::HashMap::new();
    for (node, model) in nodes.iter().zip(models) {
        for (decl, change) in node.changed_by.iter().zip(&model.changes) {
            let mut report = |message: String| errors.push(at(node.file, decl.pos, message));
            if feeds.is_empty() {
                report("changed_by requires a changes subscription".into());
            }
            let Some(relation) = model.relations.iter().find(|relation| relation.name == change.relation) else {
                report(format!("unknown relation `{}`", change.relation));
                continue;
            };
            if relation.remote.is_none() {
                report(format!("relation `{}` has no remote read", change.relation));
            }
            let node_read = relation.node == Some(NodeRead::One);
            if decl.removes_on.is_some() && !node_read {
                report("removes_on requires a via scope declared with `node: :one`".into());
            }
            let parameters: Vec<&str> = relation.text_params.iter().chain(&relation.by_columns).map(String::as_str).collect();
            match (&change.parameter, node_read) {
                (Some(_), true) => report("node read takes its id parameter from the declared change id automatically".into()),
                (Some(parameter), false) if parameters.len() != 1 || parameters[0] != parameter => report(format!("unknown or ambiguous parameter `{parameter}`")),
                (None, false) if !parameters.is_empty() => report("a parameter is required for this relation".into()),
                _ => {}
            }
            if let Some((previous_node, file, line, column)) = seen.get(change.area.as_str()) {
                if !node_read || !previous_node {
                    report(format!("duplicate change area `{}`, first declaration at file {file}:{line}:{column}", change.area));
                }
            } else {
                seen.insert(&change.area, (node_read, node.file, decl.pos.line, decl.pos.column));
            }
        }
    }
}
