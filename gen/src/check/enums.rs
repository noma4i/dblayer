use super::*;
use crate::enums::{declared_in, Site};

fn label(call: &Call) -> Option<&str> {
    match call.args.first().map(|arg| &arg.item) {
        Some(Value::Symbol(name) | Value::Const(name)) => Some(name),
        _ => None,
    }
}

fn transparent(call: &Call) -> Vec<&Call> {
    if matches!(call.word.as_str(), "shared" | "ephemeral") { call.block.iter().flatten().collect() } else { vec![call] }
}

fn find<'a>(calls: Vec<&'a Call>, path: &[String]) -> Option<&'a Call> {
    let (head, rest) = path.split_first()?;
    let call = calls.into_iter().flat_map(transparent).find(|call| label(call) == Some(head.as_str()))?;
    if rest.is_empty() { Some(call) } else { find(call.block.iter().flatten().collect(), rest) }
}

fn locate<'a>(nodes: &'a [ModelNode], site: &Site) -> Option<(usize, &'a Call)> {
    let node = nodes.iter().find(|node| node.name == site.model)?;
    let roots: Vec<&Call> = match &site.scope {
        None => node.body.iter().filter(|call| call.word == "schema").flat_map(|call| call.block.iter().flatten()).collect(),
        Some(scope) => node.body.iter().filter(|call| call.word == "remote" && label(call) == Some(scope.as_str())).flat_map(|call| call.block.iter().flatten()).filter(|call| call.word == "edge").flat_map(|call| call.block.iter().flatten()).collect(),
    };
    Some((node.file, find(roots, &site.path)?))
}

pub(super) fn check_enums(nodes: &[ModelNode], models: &[ModelSpec], errors: &mut Vec<Found>) {
    let sites = declared_in(models);
    let mut reported = HashSet::new();
    let mut fail = |index: usize, message: String| {
        if !reported.insert((index, message.clone())) { return; }
        let site = &sites[index];
        let node = nodes.iter().find(|node| node.name == site.model);
        match locate(nodes, site) {
            Some((file, call)) => errors.push(at(file, call.line, call.column, message)),
            None => errors.extend(node.map(|node| at(node.file, node.line, node.column, message))),
        }
    };
    for (index, site) in sites.iter().enumerate() {
        if site.decl.values.is_empty() { continue; }
        if let Err(message) = site.decl.check() {
            fail(index, message);
        } else if nodes.iter().any(|node| node.name == site.decl.name) {
            fail(index, format!("enum `{}` has the name of a model", site.decl.name));
        } else if let Some(earlier) = sites[..index].iter().position(|earlier| earlier.decl.name == site.decl.name && earlier.decl.values != site.decl.values) {
            let message = format!("enum `{}` is declared with different values: {} and {}", site.decl.name, sites[earlier].decl.values.join(", "), site.decl.values.join(", "));
            fail(earlier, message.clone());
            fail(index, message);
        }
    }
}
