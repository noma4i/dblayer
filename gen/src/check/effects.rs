use dblayer::value::ScalarKind;

use crate::build::scope_is_singleton;
use crate::decl::{ModelDecl, OperationDecl, ParamBase, Pos, SourceDecl, TypeRef};

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
