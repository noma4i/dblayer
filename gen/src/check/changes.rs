use dblayer::schema::{FieldKind, ModelSpec, NodeRead};
use dblayer::value::ScalarKind;

use super::{at, Found};
use crate::decl::ModelDecl;

pub(super) fn check_changes(nodes: &[ModelDecl], models: &[ModelSpec], errors: &mut Vec<Found>) {
    let mut seen: std::collections::HashMap<(usize, &str, bool), (usize, usize, usize)> = std::collections::HashMap::new();
    for (index, (node, model)) in nodes.iter().zip(models).enumerate() {
        for scope in &node.scopes {
            if let Some(preload) = &scope.preload {
                if let Some(gate) = &preload.when {
                    match models.iter().find(|candidate| candidate.key.as_str() == gate.of) {
                        None => errors.push(at(node.file, preload.pos, "preload when names an unknown model")),
                        Some(origin) if !origin.relations.iter().any(dblayer::schema::RelationSpec::is_singleton) => errors.push(at(node.file, preload.pos, "preload when requires a singleton model")),
                        Some(origin) if !origin.field(&gate.field).is_some_and(|field| matches!(field.kind, FieldKind::Scalar { kind: ScalarKind::Bool })) => errors.push(at(node.file, preload.pos, "preload when requires a bool attribute")),
                        Some(_) => {}
                    }
                }
            }
        }
        if let Some(field) = &model.server_revision {
            if !model.field(field).is_some_and(|field| matches!(field.kind, FieldKind::Scalar { kind: ScalarKind::Int }) && !crate::emit::is_nullable(field) && !field.is_local()) {
                errors.push(at(node.file, node.pos, "server_revision requires a required integer attribute"));
            }
        }
        if let (Some(field), Some((_, pos))) = (&model.cable_when, &node.cable_when) {
            if !model.relations.iter().any(dblayer::schema::RelationSpec::is_singleton) {
                errors.push(at(node.file, *pos, "cable_when requires a singleton model"));
            } else if !model.field(field).is_some_and(|field| matches!(field.kind, FieldKind::Scalar { kind: ScalarKind::Bool })) {
                errors.push(at(node.file, *pos, "cable_when requires a bool attribute"));
            }
            if nodes.iter().zip(models).take(index).any(|(_, earlier)| earlier.cable_when.is_some()) {
                errors.push(at(node.file, *pos, "duplicate cable_when"));
            }
        }
        for (decl, change) in node.changed_by.iter().zip(&model.changes) {
            let mut report = |message: String| errors.push(at(node.file, decl.pos, message));
            if let Some((file, line, column)) = seen.get(&(index, change.area.as_str(), change.observed)) {
                report(format!("duplicate change area `{}`, first declaration at file {file}:{line}:{column}", change.area));
            } else {
                seen.insert((index, &change.area, change.observed), (node.file, decl.pos.line, decl.pos.column));
            }
            if let Some(path) = &change.payload {
                if path.split('.').any(str::is_empty) { report("payload path has an empty segment".into()); }
                if change.observed || change.id_of.is_some() || change.parameter.is_some() {
                    report("payload requires a node read without id_of or parameter".into());
                }
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
            if change.payload.is_some() && !node_read { report("payload requires a via scope declared with `node: :one`".into()); }
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
