use super::*;

pub(super) fn check_changes(nodes: &[ModelNode], models: &[ModelSpec], errors: &mut Vec<Found>) {
    let feeds: Vec<_> = nodes.iter().flat_map(|node| node.body.iter().filter(|call| call.word == "changes").map(move |call| (node, call))).collect();
    for (node, call) in feeds.iter().skip(1) {
        errors.push(at(node.file, call.line, call.column, "duplicate changes subscription".into()));
    }
    let mut seen: std::collections::HashMap<&str, (bool, usize, usize, usize)> = std::collections::HashMap::new();
    for (node, model) in nodes.iter().zip(models) {
        if let Some(feed) = &model.change_feed {
            if [&feed.root, &feed.area, &feed.kind, &feed.id].iter().any(|path| path.split('.').any(str::is_empty)) {
                let call = node.body.iter().find(|call| call.word == "changes").unwrap();
                errors.push(at(node.file, call.line, call.column, "changes requires nonempty response paths".into()));
            }
        }
        for (call, change) in node.body.iter().filter(|call| call.word == "changed_by").zip(&model.changes) {
            let mut report = |message: String| errors.push(at(node.file, call.line, call.column, message));
            if feeds.is_empty() { report("changed_by requires a changes subscription".into()); }
            let Some(relation) = model.relations.iter().find(|relation| relation.name == change.relation) else {
                report(format!("unknown relation `{}`", change.relation));
                continue;
            };
            if relation.remote.is_none() { report(format!("relation `{}` has no remote read", change.relation)); }
            let node_read = relation.node == Some(dblayer::schema::NodeRead::One);
            if call.options.iter().any(|pair| pair.key == "removes_on") && (!node_read || change.relation != "node") {
                report("removes_on requires via: :node".into());
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
                seen.insert(&change.area, (node_read, node.file, call.line, call.column));
            }
        }
    }
}
