use std::collections::HashMap;

use dblayer::schema::Membership;
use dblayer::value::ScalarKind;

use super::{at, Found};
use crate::build::association_target;
use crate::decl::{AssociationKind, AttributeDecl, CopySetDecl, ModelDecl, OptimisticDecl, TypeRef};

fn source_type(child: &ModelDecl, field: &str) -> Option<TypeRef> {
    if let Some(attribute) = child.attributes.iter().find(|attribute| attribute.name == field) {
        return Some(attribute.ty.clone());
    }
    (field == "id").then_some(TypeRef::Scalar(ScalarKind::Id))
}

pub(super) fn check_derived(nodes: &[ModelDecl], errors: &mut Vec<Found>) {
    let mut writers: HashMap<(&str, &str), usize> = HashMap::new();
    for child in nodes {
        for association in child.associations.iter().filter(|association| association.kind == AssociationKind::BelongsTo) {
            let target = association_target(association);
            let Some(parent) = nodes.iter().find(|candidate| candidate.name == target) else { continue };
            if association.sets.is_empty() && !association.newer.is_empty() {
                errors.push(at(child.file, association.newer_pos.unwrap_or(association.pos), format!("`newer` of `{}` needs `sets:`", association.name)));
            }
            for field in association.newer.iter().filter(|field| source_type(child, field).is_none()) {
                errors.push(at(child.file, association.newer_pos.unwrap_or(association.pos), format!("`newer` of `{}` names unknown field `{field}` of {}", association.name, child.name)));
            }
            for (field, source) in &association.sets {
                match parent.attributes.iter().find(|attribute| &attribute.name == field) {
                    None => errors.push(at(child.file, association.pos, format!("`sets` of `{}` names unknown field `{field}` of {}", association.name, parent.name))),
                    Some(attribute) if !attribute.derived => errors.push(at(child.file, association.pos, format!("`sets` of `{}` writes `{field}`: declare it `derived: true` in {}", association.name, parent.name))),
                    Some(attribute) => {
                        *writers.entry((parent.name.as_str(), field.as_str())).or_default() += 1;
                        match source_type(child, source) {
                            None => errors.push(at(child.file, association.pos, format!("`sets` of `{}` reads unknown field `{source}` of {} for `{field}`", association.name, child.name))),
                            Some(ty) if ty != attribute.ty => errors.push(at(child.file, association.pos, format!("`sets` of `{}`: `{field}` of {} has another type than `{source}` of {}", association.name, parent.name, child.name))),
                            Some(_) => {}
                        }
                    }
                }
            }
        }
    }
    for node in nodes {
        for attribute in node.attributes.iter().filter(|attribute| attribute.derived) {
            match writers.get(&(node.name.as_str(), attribute.name.as_str())).copied().unwrap_or(0) {
                1 => {}
                0 => errors.push(at(node.file, attribute.pos, format!("derived attribute `{}` is named by no `sets:`", attribute.name))),
                count => errors.push(at(node.file, attribute.pos, format!("derived attribute `{}` is named by {count} `sets:`: a derived field has one writer", attribute.name))),
            }
        }
        check_before_answer(node, errors);
    }
}

fn check_before_answer(node: &ModelDecl, errors: &mut Vec<Found>) {
    let derived = |name: &str| node.attributes.iter().any(|attribute: &AttributeDecl| attribute.derived && attribute.name == name);
    let mut refuse = |pos, name: &str, place: &str| errors.push(at(node.file, pos, format!("derived attribute `{name}` is written by `sets:` only: it cannot be named in {place}")));
    for operation in &node.operations {
        if let Some((OptimisticDecl::Fields(fields), _)) = &operation.optimistic {
            fields.iter().filter(|(name, _, _)| derived(name)).for_each(|(name, _, pos)| refuse(*pos, name, "`optimistic`"));
        }
        if let Some(copy) = &operation.copies_receiver_to {
            copy.set.iter().filter(|(name, _, _): &&(String, CopySetDecl, _)| derived(name)).for_each(|(name, _, pos)| refuse(*pos, name, "`copies_receiver_to`"));
        }
        operation.local.iter().flatten().filter(|name| derived(name)).for_each(|name| refuse(operation.pos, name, "`local`"));
        operation.local_from.iter().filter(|(name, _, _)| derived(name)).for_each(|(name, _, pos)| refuse(*pos, name, "`local`"));
    }
    for scope in &node.scopes {
        for (_, rule, pos) in &scope.membership {
            let (Membership::Contains(field) | Membership::Equals(field)) = rule;
            if derived(field) {
                refuse(*pos, field, "`membership`");
            }
        }
    }
}
