use dblayer::schema::{ModelSpec, NodeRead};

use super::{at, Found};
use crate::decl::ModelDecl;

pub(super) fn check_changes(nodes: &[ModelDecl], models: &[ModelSpec], errors: &mut Vec<Found>) {
    let feeds: Vec<_> = nodes.iter().flat_map(|node| node.changes.iter().map(move |feed| (node, feed))).collect();
    for (node, feed) in feeds.iter().skip(1) {
        errors.push(at(node.file, feed.pos, "duplicate changes subscription"));
    }
    let mut seen: std::collections::HashMap<(usize, &str), (usize, usize, usize)> = std::collections::HashMap::new();
    for (index, (node, model)) in nodes.iter().zip(models).enumerate() {
        for (decl, change) in node.changed_by.iter().zip(&model.changes) {
            let mut report = |message: String| errors.push(at(node.file, decl.pos, message));
            if let Some((file, line, column)) = seen.get(&(index, change.area.as_str())) {
                report(format!("duplicate change area `{}`, first declaration at file {file}:{line}:{column}", change.area));
            } else {
                seen.insert((index, &change.area), (node.file, decl.pos.line, decl.pos.column));
            }
            if feeds.is_empty() {
                report("changed_by requires a changes subscription".into());
            }
            if change.observed {
                if decl.removes_on.is_some() { report("removes_on cannot be combined with `via: :observed`".into()); }
                if change.parameter.is_some() { report("`via: :observed` takes no parameter".into()); }
                if !model.relations.iter().any(|relation| relation.remote.is_some()) { report("`via: :observed` requires a model with a remote read".into()); }
                continue;
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
        }
    }
}
