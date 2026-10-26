use std::collections::HashSet;

use dblayer::value::ScalarKind;

use crate::build::scope_is_singleton;
use crate::decl::{EffectDecl, ModelDecl, OperationDecl, OptimisticDecl, ParamBase, Pos, ResponseDecl, SourceDecl, SourcePair, Spanned, TypeRef};

type Report<'a> = &'a mut dyn FnMut(Pos, String);

struct Site<'a> {
    node: &'a ModelDecl,
    all: &'a [ModelDecl],
    operation: &'a OperationDecl,
}

impl Site<'_> {
    fn parameter(&self, name: &str) -> Option<&crate::decl::ParameterDecl> {
        self.operation.parameters.iter().find(|parameter| parameter.name == name)
    }

    fn model(&self, name: &str) -> Option<&ModelDecl> {
        self.all.iter().find(|candidate| candidate.name == name)
    }

    fn id_parameter(&self, name: &str) -> bool {
        self.parameter(name).is_some_and(|parameter| parameter.ty.base == ParamBase::Id)
    }
}

fn numeric(attribute: &crate::decl::AttributeDecl) -> bool {
    matches!(attribute.ty, TypeRef::Scalar(ScalarKind::Int | ScalarKind::Num))
}

fn is_singleton(model: &ModelDecl) -> bool {
    let mut found = model.scopes.iter().filter(|scope| scope_is_singleton(scope));
    found.next().is_some() && found.next().is_none()
}

pub(super) fn used_parameter(operation: &OperationDecl, name: &str) -> bool {
    let in_source = |pair: &SourcePair| matches!(&pair.source, SourceDecl::Parameter(used) | SourceDecl::Plus(used) | SourceDecl::Minus(used) if used == name)
        || matches!(&pair.source, SourceDecl::Row { row: Some(used), .. } if used == name);
    operation.effects.iter().any(|effect| match effect {
        EffectDecl::Select { rows, with, .. } => rows.as_ref().is_some_and(|rows| rows.item == name) || with.iter().any(in_source),
        EffectDecl::Create { values, .. } => values.iter().any(in_source),
        EffectDecl::Change { row, values, .. } => row.as_ref().is_some_and(|row| row.item == name) || values.iter().any(in_source),
    })
}

pub(super) fn check_local_from(node: &ModelDecl, all: &[ModelDecl], operation: &OperationDecl, report: Report<'_>) {
    let site = Site { node, all, operation };
    for (_, source, pos) in &operation.local_from {
        source_of(&site, source, *pos, Use::Value, report);
    }
}

pub(super) fn check(node: &ModelDecl, all: &[ModelDecl], operation: &OperationDecl, report: Report<'_>) {
    if operation.effects.is_empty() {
        return;
    }
    let site = Site { node, all, operation };
    if let Some((OptimisticDecl::Off, pos)) = &operation.optimistic {
        report(*pos, "`optimistic false` contradicts the effect lines: remove it".to_string());
    }
    let mut created = HashSet::new();
    for effect in &operation.effects {
        match effect {
            EffectDecl::Select { pos, adds, scope, model, rows, with } => select(&site, (*pos, *adds), scope, model.as_deref(), rows.as_ref(), with, report),
            EffectDecl::Create { pos, model, values } => {
                if !created.insert(model.as_str()) {
                    report(*pos, format!("`create_row {model}` is declared twice"));
                }
                create(&site, *pos, model, values, report);
            }
            EffectDecl::Change { pos, model, row, values } => change(&site, *pos, model, row.as_ref(), values, report),
        }
    }
}

fn select(site: &Site<'_>, (pos, adds): (Pos, bool), scope: &str, model: Option<&str>, rows: Option<&Spanned<String>>, with: &[SourcePair], report: Report<'_>) {
    let word = if adds { "add_to" } else { "remove_from" };
    let target_name = model.unwrap_or(&site.node.name);
    let Some(target) = site.model(target_name) else {
        report(pos, format!("`{word}` names unknown model `{target_name}`"));
        return;
    };
    let Some(declared) = target.scopes.iter().find(|candidate| candidate.name == scope && candidate.is_remote()) else {
        report(pos, format!("`{word}` names `{scope}`, which is not a scope with `query` of `{target_name}`"));
        return;
    };
    match rows {
        Some(rows) if !site.id_parameter(&rows.item) => report(rows.pos, format!("`rows:` names `{0}`, declare it with `parameter :{0}, [:id]`", rows.item)),
        None if target_name != site.node.name => report(pos, format!("`{word} :{scope}` selects `{target_name}` rows: add `rows:`")),
        _ => {}
    }
    for pair in with {
        if !declared.parameters.iter().any(|parameter| parameter.name == pair.name) {
            report(pair.pos, format!("`with:` names `{}`, which is not a parameter of scope `{scope}`", pair.name));
        }
        source(site, pair, Use::Value, report);
    }
    let unbound: Vec<&str> = declared.parameters.iter().filter(|parameter| !with.iter().any(|pair| pair.name == parameter.name)).map(|parameter| parameter.name.as_str()).collect();
    if adds {
        for parameter in declared.parameters.iter().filter(|parameter| unbound.contains(&parameter.name.as_str()) && parameter.default.is_none()) {
            report(pos, format!("`add_to :{scope}` needs `with: {{ {0}: ... }}` for parameter `{0}`", parameter.name));
        }
    } else if !with.is_empty() && !unbound.is_empty() {
        report(pos, format!("`remove_from :{scope}` binds all parameters or none"));
    }
}

fn create(site: &Site<'_>, pos: Pos, model: &str, values: &[SourcePair], report: Report<'_>) {
    let Some(target) = site.model(model) else {
        report(pos, format!("`create_row` names unknown model `{model}`"));
        return;
    };
    for pair in values {
        if !target.attributes.iter().any(|attribute| attribute.name == pair.name) {
            report(pair.pos, format!("`create_row` names unknown attribute `{}` of `{model}`", pair.name));
        }
        source(site, pair, Use::Value, report);
    }
    let answered = site.operation.upserts.iter().any(|upsert| upsert.model == model) || (model == site.node.name && matches!(site.operation.response, Some(ResponseDecl::Read(_))));
    if !answered {
        report(pos, format!("`create_row {model}` is replaced by the answer: add `upsert {model}, from:`"));
    }
}

fn change(site: &Site<'_>, pos: Pos, model: &str, row: Option<&Spanned<String>>, values: &[SourcePair], report: Report<'_>) {
    let Some(target) = site.model(model) else {
        report(pos, format!("`change_row` names unknown model `{model}`"));
        return;
    };
    match row {
        Some(row) if !site.id_parameter(&row.item) => report(row.pos, format!("`row:` names `{0}`, declare it with `parameter :{0}, :id`", row.item)),
        None if !is_singleton(target) => report(pos, format!("`change_row {model}` needs `row: :param`: {model} is not a singleton")),
        _ => {}
    }
    for pair in values {
        let Some(attribute) = target.attributes.iter().find(|attribute| attribute.name == pair.name) else {
            report(pair.pos, format!("`change_row` names unknown attribute `{}` of `{model}`", pair.name));
            continue;
        };
        if matches!(pair.source, SourceDecl::Plus(_) | SourceDecl::Minus(_)) && !numeric(attribute) {
            report(pair.pos, format!("`plus:` and `minus:` need an :int or :num attribute, `{}` is not one", pair.name));
        }
        source(site, pair, Use::Change, report);
    }
}

#[derive(PartialEq)]
enum Use {
    Value,
    Change,
}

fn source(site: &Site<'_>, pair: &SourcePair, usage: Use, report: Report<'_>) {
    source_of(site, &pair.source, pair.pos, usage, report);
}

fn source_of(site: &Site<'_>, source: &SourceDecl, pos: Pos, usage: Use, report: Report<'_>) {
    let declared = |name: &str| site.parameter(name).is_some();
    match source {
        SourceDecl::Parameter(name) if !declared(name) => report(pos, format!("value `:{name}` is not declared with `parameter`")),
        SourceDecl::Owner(field) if field != "id" && !site.node.attributes.iter().any(|attribute| &attribute.name == field) => report(pos, format!("`{{ owner: :{field} }}` names unknown attribute `{field}`")),
        SourceDecl::Row { model, row, field } => match site.model(model) {
            None => report(pos, format!("`{{ of: {model} }}` names unknown model `{model}`")),
            Some(target) => {
                if field != "id" && !target.attributes.iter().any(|attribute| &attribute.name == field) {
                    report(pos, format!("`{{ of: {model} }}` names unknown attribute `{field}`"));
                }
                match row {
                    Some(row) if !site.id_parameter(row) => report(pos, format!("`row:` names `{row}`, declare it with `parameter :{row}, :id`")),
                    None if !is_singleton(target) => report(pos, format!("`{{ of: {model} }}` needs `row: :param`: {model} is not a singleton")),
                    _ => {}
                }
            }
        },
        SourceDecl::Plus(name) | SourceDecl::Minus(name) => {
            if usage == Use::Value {
                report(pos, "`plus:` and `minus:` belong to `change_row`".to_string());
            }
            if !site.parameter(name).is_some_and(|parameter| matches!(parameter.ty.base, ParamBase::Int | ParamBase::Long | ParamBase::Num)) {
                report(pos, format!("`{name}` must be a number parameter declared with `parameter :{name}, :int`"));
            }
        }
        _ => {}
    }
}
