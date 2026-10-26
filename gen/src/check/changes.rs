use dblayer::schema::{FieldKind, ModelSpec, NodeRead};
use dblayer::value::ScalarKind;

use super::{at, Found};
use crate::decl::ModelDecl;

pub(super) fn check_changes(nodes: &[ModelDecl], models: &[ModelSpec], errors: &mut Vec<Found>) {
    let feeds: Vec<_> = nodes.iter().flat_map(|node| node.changes.iter().map(move |feed| (node, feed))).collect();
    for (node, feed) in feeds.iter().skip(1) {
        errors.push(at(node.file, feed.pos, "duplicate changes subscription"));
    }
    let mut seen: std::collections::HashMap<(usize, &str, bool), (usize, usize, usize)> = std::collections::HashMap::new();
    for (index, (node, model)) in nodes.iter().zip(models).enumerate() {
        for (decl, change) in node.changed_by.iter().zip(&model.changes) {
            let mut report = |message: String| errors.push(at(node.file, decl.pos, message));
            if let Some((file, line, column)) = seen.get(&(index, change.area.as_str(), change.observed)) {
                report(format!("duplicate change area `{}`, first declaration at file {file}:{line}:{column}", change.area));
            } else {
                seen.insert((index, &change.area, change.observed), (node.file, decl.pos.line, decl.pos.column));
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
            if let Some((source, field)) = &decl.id_of {
                if !node_read { report("`id_of` requires a via scope declared with `node: :one`".into()); }
                if decl.removes_on.is_some() { report("removes_on cannot be combined with `id_of`".into()); }
                match models.iter().find(|candidate| candidate.key.as_str() == source) {
                    None => report(format!("`id_of` names unknown model `{source}`")),
                    Some(origin) => {
                        if !origin.changes.iter().any(|read| read.area == change.area && !read.observed && read.id_of.is_none() && origin.relations.iter().any(|relation| relation.name == read.relation && relation.node == Some(NodeRead::One))) {
                            report(format!("`id_of` needs {source} to declare area `{}` with a node read", change.area));
                        }
                        if !origin.field(field).is_some_and(|found| matches!(found.kind, FieldKind::Scalar { kind: ScalarKind::Id })) {
                            report(format!("`id_of` names `{source}.{field}` which is not an id attribute"));
                        }
                    }
                }
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
