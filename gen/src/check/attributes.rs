use std::collections::HashSet;

use dblayer::value::ScalarKind;

use super::{at, Found};
use crate::decl::{AttributeBlock, AttributeDecl, ModelDecl, Pos, TypeRef};

pub(super) struct AttributeCheck<'a, F: FnMut(Pos, String)> {
    pub node: &'a ModelDecl,
    pub report: &'a mut F,
}

impl<F: FnMut(Pos, String)> AttributeCheck<'_, F> {
    pub fn run(self) {
        let Self { node, report } = self;
        let mut names = HashSet::new();
        for attribute in &node.attributes {
            if !names.insert(attribute.name.as_str()) {
                report(attribute.pos, format!("attribute `{}` is declared twice", attribute.name));
            }
            check_local(attribute, report);
            check_block(attribute, report);
        }
        for group in &node.ephemeral {
            for attribute in &group.attributes {
                if !names.insert(attribute.name.as_str()) {
                    report(attribute.pos, format!("attribute `{}` is declared twice", attribute.name));
                }
                check_ephemeral(attribute, report);
                check_block(attribute, report);
            }
        }
        let mut searched = HashSet::new();
        for name in &node.search {
            let searchable = node.attributes.iter().find(|attribute| attribute.name == name.item).map(|attribute| matches!(attribute.ty, TypeRef::Scalar(ScalarKind::Str) | TypeRef::List(_)));
            match searchable {
                None => report(name.pos, "search needs a declared attribute".to_string()),
                Some(false) => report(name.pos, format!("search `{}` needs a :string or list attribute", name.item)),
                Some(true) if !searched.insert(name.item.as_str()) => report(name.pos, format!("search `{}` is declared twice", name.item)),
                Some(true) => {}
            }
        }
    }
}

fn check_local(attribute: &AttributeDecl, report: &mut impl FnMut(Pos, String)) {
    if !attribute.local {
        return;
    }
    let name = &attribute.name;
    if attribute.merge.is_some() || attribute.default.is_some() || attribute.from.is_some() || attribute.id_from.is_some() {
        report(attribute.pos, format!("local attribute `{name}` cannot set `merge`, `default`, `from` or `id_from`"));
    }
}

fn check_ephemeral(attribute: &AttributeDecl, report: &mut impl FnMut(Pos, String)) {
    let name = &attribute.name;
    if attribute.reset_after.is_some() && attribute.default.is_none() {
        report(attribute.pos, "`reset_after` needs `default:`".to_string());
    } else if attribute.default.is_none() && !attribute.null {
        report(attribute.pos, format!("ephemeral attribute `{name}` needs `default:` or `null: true`"));
    }
}

pub(super) fn check_edges(edges: &[AttributeDecl], report: &mut impl FnMut(Pos, String)) {
    let mut names = HashSet::new();
    for edge in edges {
        let name = &edge.name;
        if !names.insert(name.as_str()) {
            report(edge.pos, format!("edge `{name}` is declared twice"));
        } else if !edge.null && !edge.optional {
            report(edge.pos, format!("edge `{name}` needs `null: true` or `optional: true`"));
        } else if edge.parse_json {
            report(edge.pos, format!("edge `{name}` cannot set `parse`"));
        } else if edge.default.is_some() {
            report(edge.pos, format!("edge `{name}` cannot set `default`"));
        }
        check_block(edge, report);
    }
}

fn check_block(owner: &AttributeDecl, report: &mut impl FnMut(Pos, String)) {
    match &owner.block {
        AttributeBlock::None => {}
        AttributeBlock::Fields(inner) => check_fields(inner, report),
        AttributeBlock::Union { shared, variants } => check_union(owner, shared.as_ref().map(|shared| shared.item.as_slice()), variants, report),
    }
}

fn check_fields(attributes: &[AttributeDecl], report: &mut impl FnMut(Pos, String)) {
    let mut names = HashSet::new();
    for attribute in attributes {
        if !names.insert(attribute.name.as_str()) {
            report(attribute.pos, format!("attribute `{}` is declared twice", attribute.name));
        }
        check_block(attribute, report);
    }
}

fn check_union(owner: &AttributeDecl, shared: Option<&[AttributeDecl]>, variants: &[crate::decl::VariantDecl], report: &mut impl FnMut(Pos, String)) {
    let shared_names: Vec<&str> = shared.iter().flat_map(|fields| fields.iter()).map(|field| field.name.as_str()).collect();
    if let Some(fields) = shared {
        check_fields(fields, report);
        for field in fields.iter().filter(|field| field.name == "kind") {
            report(field.pos, "`kind` is reserved: a union with `shared` fields already has a `kind` property".to_string());
        }
    }
    let mut typenames: Vec<&str> = Vec::new();
    let mut repeated = false;
    for variant in variants {
        if typenames.contains(&variant.type_name.as_str()) {
            repeated = true;
            report(variant.pos, format!("variant `{}` is declared twice", variant.type_name));
            continue;
        }
        typenames.push(&variant.type_name);
        let Some(fields) = &variant.fields else { continue };
        if fields.is_empty() && !shared_names.is_empty() {
            continue;
        }
        if fields.is_empty() {
            report(variant.pos, format!("`{}` block needs at least 1 attribute", variant.type_name));
        }
        check_fields(fields, report);
        for field in fields.iter().filter(|field| shared_names.contains(&field.name.as_str())) {
            report(field.pos, format!("attribute `{}` is declared twice: it is also a `shared` attribute", field.name));
        }
    }
    if repeated {
        return;
    }
    let union = owner_type(owner);
    if let Err(message) = crate::unions::check_cases(union, &crate::unions::cases_of(variants)) {
        report(owner.pos, message);
    }
}

fn owner_type(owner: &AttributeDecl) -> &str {
    match &owner.ty {
        TypeRef::Named(name) | TypeRef::NamedList(name) => name,
        _ => &owner.name,
    }
}

struct TypeUse<'a> {
    name: &'a str,
    pos: Pos,
    file: usize,
    declares: bool,
}

fn collect_types<'a>(attributes: &'a [AttributeDecl], file: usize, out: &mut Vec<TypeUse<'a>>) {
    for attribute in attributes {
        let (TypeRef::Named(name) | TypeRef::NamedList(name)) = &attribute.ty else { continue };
        match &attribute.block {
            AttributeBlock::None => out.push(TypeUse { name, pos: attribute.pos, file, declares: false }),
            AttributeBlock::Fields(inner) => {
                out.push(TypeUse { name, pos: attribute.pos, file, declares: true });
                collect_types(inner, file, out);
            }
            AttributeBlock::Union { shared, variants } => {
                out.push(TypeUse { name, pos: attribute.pos, file, declares: true });
                if let Some(shared) = shared {
                    collect_types(&shared.item, file, out);
                }
                for variant in variants {
                    out.push(TypeUse { name: &variant.type_name, pos: variant.pos, file, declares: variant.fields.is_some() });
                    if let Some(fields) = &variant.fields {
                        collect_types(fields, file, out);
                    }
                }
            }
        }
    }
}

pub(super) fn check_types(nodes: &[ModelDecl], errors: &mut Vec<Found>) {
    let mut uses = Vec::new();
    for node in nodes {
        collect_types(&node.attributes, node.file, &mut uses);
    }
    let mut declared = HashSet::new();
    for item in uses.iter().filter(|item| item.declares) {
        if !declared.insert(item.name) {
            errors.push(at(item.file, item.pos, format!("type `{}` is declared twice", item.name)));
        }
        if nodes.iter().any(|node| node.name == item.name) {
            errors.push(at(item.file, item.pos, format!("type `{}` has the name of a model", item.name)));
        }
    }
    for item in uses.iter().filter(|item| !item.declares && !declared.contains(item.name)) {
        errors.push(at(item.file, item.pos, format!("type `{}` is not declared by any `do ... end` block", item.name)));
    }
}
