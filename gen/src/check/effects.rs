use dblayer::value::ScalarKind;

use crate::build::scope_is_singleton;
use crate::decl::{ModelDecl, OperationDecl, OptimisticDecl, ParamBase, Pos, SourceDecl, TypeRef};

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

fn is_singleton(model: &ModelDecl) -> bool {
    let mut found = model.scopes.iter().filter(|scope| scope_is_singleton(scope));
    found.next().is_some() && found.next().is_none()
}

pub(super) fn check_local_from(node: &ModelDecl, all: &[ModelDecl], operation: &OperationDecl, report: Report<'_>) {
    let site = Site { node, all, operation };
    for (field, source, pos) in &operation.local_from {
        source_of(&site, source, *pos, report);
        if let SourceDecl::Row { model, field: from, .. } = source {
            let target = node.attributes.iter().find(|attribute| &attribute.name == field);
            let origin = site.model(model).and_then(|model| model.attributes.iter().find(|attribute| &attribute.name == from));
            if let (Some(target), Some(origin)) = (target, origin) {
                if target.ty != origin.ty || (origin.null && !target.null) {
                    report(*pos, format!("local attribute `{field}` cannot accept `{model}.{from}`"));
                }
            } else if from == "id" && target.is_some_and(|target| target.ty != TypeRef::Scalar(ScalarKind::Id)) {
                report(*pos, format!("local attribute `{field}` must accept an id"));
            }
        }
    }
}

pub(super) fn check_optimistic_source(node: &ModelDecl, all: &[ModelDecl], operation: &OperationDecl, source: &SourceDecl, pos: Pos, report: Report<'_>) {
    source_of(&Site { node, all, operation }, source, pos, report);
}

fn source_of(site: &Site<'_>, source: &SourceDecl, pos: Pos, report: Report<'_>) {
    match source {
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
        _ => {}
    }
}

/// The closed set of effects of an operation (`removes_receiver`, `copies_receiver_to`, `joins_scope`, `leaves_scope`): one effect per operation, and every name it uses exists.
pub(super) fn check_closed_effects(node: &ModelDecl, all: &[ModelDecl], operation: &OperationDecl, report: Report<'_>) {
    let kinds = usize::from(operation.removes_receiver.is_some()) + usize::from(operation.copies_receiver_to.is_some()) + operation.scope_effects.len();
    if kinds > 1 {
        report(operation.pos, format!("operation `{}` declares more than one effect: `removes_receiver`, `copies_receiver_to`, `joins_scope` and `leaves_scope` do not combine", operation.name));
    }
    if kinds > 0 && matches!(&operation.optimistic, Some((OptimisticDecl::Fields(_), _))) {
        report(operation.pos, format!("operation `{}` declares an effect and `optimistic` fields: the effect is the optimistic change, write `optimistic false`", operation.name));
    }
    let site = Site { node, all, operation };
    if let Some(copy) = &operation.copies_receiver_to {
        if !site.id_parameter(&copy.arg) {
            report(copy.pos, format!("`copies_receiver_to` names `:{}`, which must be an id parameter", copy.arg));
        }
        if !node.attributes.iter().any(|attribute| attribute.name == copy.field) {
            report(copy.pos, format!("`copies_receiver_to` names unknown attribute `{}`", copy.field));
        }
        for (field, value, pos) in &copy.set {
            if !node.attributes.iter().any(|attribute| &attribute.name == field) {
                report(*pos, format!("`copies_receiver_to` sets unknown attribute `{field}`"));
            }
            if let crate::decl::CopySetDecl::Source(source) = value { source_of(&site, source, *pos, report); }
        }
    }
    for effect in &operation.scope_effects {
        let Some(target) = site.model(&effect.model) else {
            report(effect.pos, format!("`{}` names unknown model `{}`", if effect.joins { "joins_scope" } else { "leaves_scope" }, effect.model));
            continue;
        };
        let Some(scope) = target.scopes.iter().find(|scope| scope.name == effect.scope) else {
            report(effect.pos, format!("model `{}` has no scope `{}`", effect.model, effect.scope));
            continue;
        };
        let known = |name: &str| scope.parameters.iter().any(|parameter| parameter.name == name);
        if !known(&effect.order) {
            report(effect.pos, format!("scope `{}` has no parameter `{}`", effect.scope, effect.order));
        }
        for (name, source) in &effect.with {
            if !known(name) {
                report(effect.pos, format!("scope `{}` has no parameter `{name}`", effect.scope));
            }
            if source != "id" && site.parameter(source).is_none() {
                report(effect.pos, format!("`with` reads `:{source}`, which is not `:id` or a declared parameter"));
            }
        }
        let missing: Vec<&str> = scope.parameters.iter().filter(|parameter| parameter.name != effect.order && parameter.default.is_none() && !effect.with.iter().any(|(name, _)| name == &parameter.name)).map(|parameter| parameter.name.as_str()).collect();
        if !missing.is_empty() {
            report(effect.pos, format!("scope `{}` needs `with:` for {}", effect.scope, missing.join(", ")));
        }
        match site.parameter(&effect.rows) {
            Some(parameter) if parameter.ty.base == ParamBase::Id => {}
            _ => report(effect.pos, format!("`rows` names `:{}`, which must be an id parameter or a list of ids", effect.rows)),
        }
    }
}
