use std::collections::HashSet;

use dblayer::schema::ModelSpec;

use super::{at, Found};
use crate::decl::{AttributeBlock, AttributeDecl, ModelDecl, Pos};
use crate::enums::{declared_in, Site};

fn locate(attributes: &[&AttributeDecl], path: &[String]) -> Option<Pos> {
    let (head, rest) = path.split_first()?;
    if let Some(attribute) = attributes.iter().find(|attribute| &attribute.name == head) {
        if rest.is_empty() {
            return Some(attribute.pos);
        }
        return match &attribute.block {
            AttributeBlock::None => None,
            AttributeBlock::Fields(inner) => locate(&inner.iter().collect::<Vec<_>>(), rest),
            AttributeBlock::Union { shared, variants } => {
                let (label, deeper) = rest.split_first()?;
                if let Some(variant) = variants.iter().find(|variant| &variant.type_name == label) {
                    return if deeper.is_empty() { Some(variant.pos) } else { locate(&variant.fields.iter().flatten().collect::<Vec<_>>(), deeper) };
                }
                locate(&shared.iter().flat_map(|shared| shared.item.iter()).collect::<Vec<_>>(), rest)
            }
        };
    }
    None
}

fn site_pos(node: &ModelDecl, site: &Site) -> Option<Pos> {
    let roots: Vec<&AttributeDecl> = match &site.scope {
        None => node.attributes.iter().chain(node.ephemeral.iter().flat_map(|group| &group.attributes)).collect(),
        Some(scope) => node.scopes.iter().filter(|candidate| &candidate.name == scope).flat_map(|candidate| candidate.edges.iter()).collect(),
    };
    locate(&roots, &site.path)
}

pub(super) fn check_enums(nodes: &[ModelDecl], models: &[ModelSpec], errors: &mut Vec<Found>) {
    let sites = declared_in(models);
    let mut reported = HashSet::new();
    let mut fail = |index: usize, message: String| {
        if !reported.insert((index, message.clone())) {
            return;
        }
        let site = &sites[index];
        let Some(node) = nodes.iter().find(|node| node.name == site.model) else { return };
        errors.push(at(node.file, site_pos(node, site).unwrap_or(node.pos), message));
    };
    for (index, site) in sites.iter().enumerate() {
        if site.decl.values.is_empty() {
            continue;
        }
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
