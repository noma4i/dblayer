use super::*;

pub(super) fn check_scope(node: &ModelDecl, all: &[ModelDecl], scope: &ScopeDecl, report: &mut impl FnMut(Pos, String)) {
    let name = &scope.name;
    if scope.marker.is_some() || scope.tail.is_some() {
        if scope.marker.is_none() || scope.tail.is_none() || scope.paginate.is_none() || !scope.is_remote() || scope.node.is_some() {
            report(scope.pos, "marker and tail need one paginated remote scope".into());
        }
        if scope.paginate.as_ref().is_some_and(|page| page.max_pages.is_some()) { report(scope.pos, "marker coverage cannot stop at max_pages".into()); }
        if let Some(marker) = &scope.marker {
            let names: HashSet<_> = marker.fields.iter().map(|field| &field.item).collect();
            if names.len() != marker.fields.len() || !names.iter().any(|name| name.as_str() == "revision") { report(marker.pos, "marker fields must be unique and include revision".into()); }
        }
    }
    let mut names = HashSet::new();
    for parameter in &scope.parameters {
        if !names.insert(parameter.name.as_str()) {
            report(parameter.pos, format!("scope `{name}`: parameter `{}` is declared twice", parameter.name));
        }
        if scope.node.is_none() && parameter.name == dblayer::schema::NODE_PARAM {
            report(parameter.pos, format!("scope `{name}`: parameter `{}` is reserved for the `node` read", parameter.name));
        }
    }
    let linked: Vec<&str> = scope.where_.iter().flat_map(|clause| clause.pairs.iter().map(|(_, parameter)| parameter.item.as_str())).collect();
    if let Some(clause) = &scope.where_ {
        let mut columns = HashSet::new();
        for (column, parameter) in &clause.pairs {
            if !scope.parameters.iter().any(|candidate| candidate.name == parameter.item) {
                report(parameter.pos, format!("scope `{name}`: where value `:{}` is not declared with `parameter`", parameter.item));
            }
            if !columns.insert(column.item.as_str()) {
                report(column.pos, format!("scope `{name}`: where column `{}` is given twice", column.item));
            }
        }
    }
    let mut memberships = HashSet::new();
    for (parameter, rule, pos) in &scope.membership {
        use dblayer::schema::Membership;
        let field = match rule { Membership::Contains(field) | Membership::Equals(field) => field };
        if !memberships.insert(parameter) || linked.contains(&parameter.as_str()) {
            report(*pos, format!("scope `{name}`: membership parameter `{parameter}` is declared twice or conflicts with where"));
        }
        let declared = scope.parameters.iter().find(|candidate| &candidate.name == parameter);
        let attribute = node.attributes.iter().find(|attribute| &attribute.name == field);
        let fits = match (declared, attribute, rule) {
            (Some(parameter), Some(attribute), rule) if !parameter.ty.list && !parameter.null => {
                let scalar = match (&attribute.ty, rule) {
                    (TypeRef::List(crate::decl::ListElement::Scalar(kind)), Membership::Contains(_)) => Some(*kind),
                    (TypeRef::Scalar(kind), Membership::Equals(_)) => Some(*kind),
                    (TypeRef::Enum, Membership::Equals(_)) | (TypeRef::List(crate::decl::ListElement::Enum), Membership::Contains(_)) => Some(ScalarKind::Str),
                    _ => None,
                };
                match (&parameter.ty.base, scalar) {
                    (ParamBase::String, Some(ScalarKind::Str)) | (ParamBase::Id, Some(ScalarKind::Id)) | (ParamBase::Int, Some(ScalarKind::Int)) | (ParamBase::Bool, Some(ScalarKind::Bool)) => true,
                    (ParamBase::Named(kind), Some(ScalarKind::Str)) => attribute.as_name.as_ref() == Some(kind),
                    _ => false,
                }
            }
            _ => false,
        };
        if !fits { report(*pos, format!("scope `{name}`: membership `{parameter}` must match the type of `{field}`")); }
    }
    check_row_rules(node, scope, &linked, report);
    for parameter in &scope.parameters {
        let is_linked = linked.contains(&parameter.name.as_str());
        if !scope.is_remote() && !is_linked {
            report(parameter.pos, format!("scope `{name}`: parameter `:{}` is not used in `where`", parameter.name));
        }
        if is_linked && (parameter.null || parameter.default.is_some()) {
            report(parameter.pos, format!("scope `{name}`: parameter `:{}` filters a column: `null` and `default` apply to server-only parameters", parameter.name));
        }
        if scope.node.is_some() {
            continue;
        }
        check_scope_parameter(scope, parameter, is_linked, node, report);
    }
    if let Some(order) = &scope.order {
        let mut seen = HashSet::new();
        for (column, _, pos) in &order.fields {
            if !seen.insert(column.as_str()) {
                report(*pos, format!("scope `{name}`: order column `{column}` is given twice"));
            }
        }
    }
    if let Some(variables) = &scope.variables {
        for binding in variables {
            if let VariableValue::Parameter(parameter) = &binding.value {
                if !scope.parameters.iter().any(|candidate| &candidate.name == parameter) {
                    report(binding.pos, format!("scope `{name}`: variable `{}` names `:{parameter}`, which is not declared with `parameter`", binding.name));
                }
            }
        }
    }
    if let Some(preload) = &scope.preload {
        check_preload(scope, preload, all, report);
    }
    if let Some(pos) = scope.refresh_on_foreground {
        if !scope.is_remote() {
            report(pos, format!("scope `{name}`: `refresh_on_foreground: true` needs a scope with `query`"));
        }
    }
    if !scope.is_remote() {
        if scope.members_complete { report(scope.pos, "`members_complete` needs a paginated remote scope".into()); }
        return;
    }
    if scope.members_complete && (scope.paginate.is_none() || scope.paginate.as_ref().is_some_and(|page| page.max_pages.is_some())) {
        report(scope.pos, "`members_complete` needs pagination without `max_pages`".into());
    }
    check_remote_scope(node, scope, report);
}

pub(super) fn check_preload(scope: &ScopeDecl, preload: &PreloadDecl, all: &[ModelDecl], report: &mut impl FnMut(Pos, String)) {
    let name = &scope.name;
    if scope.node.is_some() {
        report(preload.pos, format!("scope `{name}`: `preload` does not apply to a `node` scope"));
        return;
    }
    let mut keys = HashSet::new();
    if preload.limit.is_none() && preload.values.iter().any(|(_, choice)| matches!(choice, PreloadChoice::RowsOf(_))) {
        report(preload.pos, format!("scope `{name}`: derived `preload` needs a positive `limit`"));
    }
    for (key, choice) in &preload.values {
        if !keys.insert(key.item.as_str()) {
            report(key.pos, format!("scope `{name}`: `preload` lists `{}` twice", key.item));
        }
        let Some(parameter) = scope.parameters.iter().find(|parameter| parameter.name == key.item) else {
            report(key.pos, format!("scope `{name}`: `preload` names `{}`, which is not declared with `parameter`", key.item));
            continue;
        };
        let values = match choice {
            PreloadChoice::Listed(values) => values,
            PreloadChoice::RowsOf(source) => {
                check_preload_source(scope, parameter, source, all, report);
                continue;
            }
        };
        if values.is_empty() {
            report(key.pos, format!("scope `{name}`: `preload {}:` needs at least 1 value", key.item));
        }
        let mut seen = Vec::new();
        for value in values {
            let fits = match (&parameter.ty.base, parameter.ty.list, &value.item) {
                (_, true, _) => false,
                (ParamBase::Int | ParamBase::Long, _, serde_json::Value::Number(_)) | (ParamBase::Bool, _, serde_json::Value::Bool(_)) => true,
                (ParamBase::String | ParamBase::Id | ParamBase::Named(_), _, serde_json::Value::String(_)) => true,
                _ => false,
            };
            if !fits {
                report(value.pos, format!("scope `{name}`: `preload {}:` value does not match the type {} of the parameter", key.item, parameter.ty.describe()));
            } else if seen.contains(&&value.item) {
                report(value.pos, format!("scope `{name}`: `preload {}:` lists a value twice", key.item));
            }
            seen.push(&value.item);
        }
    }
    for parameter in &scope.parameters {
        if !keys.contains(parameter.name.as_str()) {
            report(preload.pos, format!("scope `{name}`: `preload` needs a list of values for parameter `{}`", parameter.name));
        }
    }
}

pub(super) fn check_preload_source(scope: &ScopeDecl, parameter: &ParameterDecl, source: &Spanned<String>, all: &[ModelDecl], report: &mut impl FnMut(Pos, String)) {
    let name = &scope.name;
    if !matches!((&parameter.ty.base, parameter.ty.list), (ParamBase::String | ParamBase::Id | ParamBase::Named(_), false)) {
        report(source.pos, format!("scope `{name}`: `preload {}: {{ from: }}` takes row ids, but the parameter is {}", parameter.name, parameter.ty.describe()));
    }
    let (model, relation) = source.item.split_once('.').expect("lowering keeps only `Model.scope` sources");
    let Some(model) = all.iter().find(|candidate| candidate.name == model) else {
        report(source.pos, format!("scope `{name}`: `preload` source names unknown model `{model}`"));
        return;
    };
    let Some(found) = model.scopes.iter().find(|candidate| candidate.name == relation) else {
        report(source.pos, format!("scope `{name}`: `preload` source model `{}` has no scope `{relation}`", model.name));
        return;
    };
    if found.node.is_some() || !found.parameters.is_empty() {
        report(source.pos, format!("scope `{name}`: `preload` source `{}` must be a list without parameters", source.item));
    }
}

pub(super) fn check_scope_parameter(scope: &ScopeDecl, parameter: &ParameterDecl, linked: bool, node: &ModelDecl, report: &mut impl FnMut(Pos, String)) {
    let name = &scope.name;
    let text_like = matches!((&parameter.ty.base, parameter.ty.list), (ParamBase::String | ParamBase::Id | ParamBase::Named(_), false));
    let typed = matches!((&parameter.ty.base, parameter.ty.list), (ParamBase::Int | ParamBase::Bool, false));
    if linked {
        let Some((column, _)) = scope.where_.iter().flat_map(|clause| &clause.pairs).find(|(_, linked)| linked.item == parameter.name) else { return };
        let Some(attribute) = node.attributes.iter().find(|attribute| attribute.name == column.item) else { return };
        let fits = match (&attribute.ty, &parameter.ty.base, parameter.ty.list) {
            (TypeRef::Scalar(ScalarKind::Id | ScalarKind::Str), ParamBase::Id | ParamBase::String, false) => true,
            (TypeRef::Scalar(ScalarKind::Int), ParamBase::Int, false) => true,
            (TypeRef::Scalar(ScalarKind::Num), ParamBase::Num, false) => true,
            (TypeRef::Scalar(ScalarKind::Bool), ParamBase::Bool, false) => true,
            (TypeRef::Scalar(ScalarKind::Date), ParamBase::String, false) => true,
            (TypeRef::Enum, ParamBase::String | ParamBase::Named(_), false) => true,
            (TypeRef::Scalar(_), _, _) => false,
            _ => true,
        };
        if !fits {
            report(parameter.pos, format!("scope `{name}`: parameter `:{}` is {} but column `{}` is a different type", parameter.name, parameter.ty.describe(), column.item));
        }
        return;
    }
    if !(text_like || typed) {
        report(parameter.pos, format!("scope `{name}`: a server-only parameter is :string, :id, :int, :bool or an enum name"));
        return;
    }
    let Some(default) = &parameter.default else { return };
    let fits = matches!((&parameter.ty.base, parameter.ty.list, &default.item), (ParamBase::Named(_), false, serde_json::Value::String(_)));
    if !fits {
        report(default.pos, format!("scope `{name}`: default of `:{}` does not match its type {}: a default belongs to an enum parameter and is one of its values", parameter.name, parameter.ty.describe()));
    }
}

pub(super) fn check_row_rules(node: &ModelDecl, scope: &ScopeDecl, linked: &[&str], report: &mut impl FnMut(Pos, String)) {
    use dblayer::schema::RowTest;
    let name = &scope.name;
    let mut fields = HashSet::new();
    for (rule, pos) in &scope.row_rules {
        if !fields.insert(rule.field.as_str()) {
            report(*pos, format!("scope `{name}`: `matches` field `{}` is given twice", rule.field));
        }
        let Some(attribute) = node.attributes.iter().find(|attribute| attribute.name == rule.field) else {
            report(*pos, format!("scope `{name}`: `matches` field `{}` is not an attribute", rule.field));
            continue;
        };
        let literal_fits = |literal: &serde_json::Value| match (&attribute.ty, literal) {
            (TypeRef::Enum, serde_json::Value::String(text)) => attribute.values.as_ref().is_none_or(|values| values.contains(text)),
            (TypeRef::Scalar(ScalarKind::Str | ScalarKind::Id), serde_json::Value::String(_)) => true,
            (TypeRef::Scalar(ScalarKind::Int), serde_json::Value::Number(number)) => number.is_i64(),
            (TypeRef::Scalar(ScalarKind::Bool), serde_json::Value::Bool(_)) => true,
            _ => false,
        };
        match &rule.test {
            RowTest::In { values } if !values.iter().all(literal_fits) => report(*pos, format!("scope `{name}`: `matches {}` values must match the type of `{}`", rule.field, rule.field)),
            RowTest::Equals { value } if !literal_fits(value) => report(*pos, format!("scope `{name}`: `matches {}` value must match the type of `{}`", rule.field, rule.field)),
            RowTest::GreaterThan { parameter } => {
                let declared = scope.parameters.iter().find(|candidate| &candidate.name == parameter);
                let int_parameter = declared.is_some_and(|declared| !declared.ty.list && !declared.null && declared.default.is_none() && matches!(declared.ty.base, ParamBase::Int));
                if !int_parameter || !matches!(attribute.ty, TypeRef::Scalar(ScalarKind::Int)) || linked.contains(&parameter.as_str()) || scope.membership.iter().any(|(candidate, _, _)| candidate == parameter) {
                    report(*pos, format!("scope `{name}`: `matches {} greater_than: :{parameter}` needs an `:int` parameter that only this rule uses and an `:int` attribute", rule.field));
                }
            }
            _ => {}
        }
    }
    let thresholds = scope.row_rules.iter().filter(|(rule, _)| matches!(rule.test, RowTest::GreaterThan { .. })).count();
    let distinct: HashSet<_> = scope.row_rules.iter().flat_map(|(rule, _)| match &rule.test { RowTest::GreaterThan { parameter } => Some(parameter), _ => None }).collect();
    if distinct.len() != thresholds {
        report(scope.row_rules[0].1, format!("scope `{name}`: a parameter can be used by one `greater_than` rule only"));
    }
}

pub(super) fn check_remote_scope(node: &ModelDecl, scope: &ScopeDecl, report: &mut impl FnMut(Pos, String)) {
    let name = &scope.name;
    let paginated = scope.paginate.is_some();
    let listed = scope.response.as_ref().is_some_and(|read| read.shape == Some(ResponseShape::List));
    if paginated && listed {
        report(scope.paginate.as_ref().map_or(scope.pos, |paginate| paginate.pos), format!("scope `{name}`: `paginate` and `response shape: :list` cannot be used together"));
    }
    if let Some(total) = &scope.total {
        if !paginated {
            report(total.pos, "`total` needs `paginate`".to_string());
        }
    }
    let linked = scope.where_.is_some() || !scope.membership.is_empty();
    let server_only = scope.parameters.iter().any(|parameter| !scope.where_.iter().flat_map(|clause| &clause.pairs).any(|(_, linked)| linked.item == parameter.name) && !scope.membership.iter().any(|(name, _, _)| name == &parameter.name) && !scope.row_rules.iter().any(|(rule, _)| matches!(&rule.test, dblayer::schema::RowTest::GreaterThan { parameter: name } if name == &parameter.name)));
    if let Some(insert) = &scope.insert_on {
        let mut sorted: Vec<_> = insert.item.iter().map(|source| *source as u8).collect();
        sorted.sort_unstable();
        let create = crate::decl::InsertOn::Create as u8;
        let events = [crate::decl::InsertOn::Event as u8, crate::decl::InsertOn::Response as u8];
        let automatic = !sorted.contains(&create);
        let without_node: Vec<_> = sorted.iter().copied().filter(|value| *value != crate::decl::InsertOn::Node as u8).collect();
        if sorted != [create] && (!automatic || (!without_node.is_empty() && without_node != events)) {
            report(insert.pos, format!("scope `{name}`: `insert_on` is `[:create]` or `[:event, :response]`, with optional `:node`, or `[:node]`"));
        } else if insert.item.contains(&crate::decl::InsertOn::Node) && !node.scopes.iter().any(|candidate| candidate.node == Some(NodeRead::One)) {
            report(insert.pos, format!("scope `{name}`: `insert_on` with `:node` needs the `node` scope of the model"));
        } else if automatic && !linked {
            report(insert.pos, format!("scope `{name}`: `insert_on: [:event, :response]` joins rows by their `where` columns and needs `where`"));
        } else if automatic && server_only {
            report(insert.pos, format!("scope `{name}`: `insert_on: [:event, :response]` joins rows by their `where` columns and a scope with server-only parameters has none, remove `insert_on:` and the server answer decides the members"));
        }
    }
    if let Some(pos) = scope.stable_order {
        if scope.order.is_none() || scope.where_.is_none() || server_only {
            report(pos, format!("scope `{name}`: `stable_order: true` keeps the shown order of the rows that `where` selects and needs `order` and `where`, without server-only parameters"));
        }
    }
    if !paginated {
        for edge in &scope.edges {
            report(edge.pos, format!("edge `{}` needs `paginate` in its scope", edge.name));
        }
    }
    attributes::check_edges(&scope.edges, report);
    match scope.node {
        Some(NodeRead::One) => check_node_one(scope, report),
        Some(NodeRead::Many) => check_node_many(scope, report),
        None => {}
    }
}

pub(super) fn check_node_one(scope: &ScopeDecl, report: &mut impl FnMut(Pos, String)) {
    let name = &scope.name;
    if scope.parameters.len() != 1 || scope.parameters[0].ty.base != ParamBase::Id || scope.parameters[0].ty.list || scope.parameters[0].null || scope.parameters[0].default.is_some() {
        report(scope.pos, format!("scope `{name}`: a `node: :one` scope has exactly one parameter, declared as `parameter :id, :id`"));
    }
    let bound = scope.variables.as_ref().is_some_and(|variables| variables.len() == 1 && matches!(&variables[0].value, VariableValue::Parameter(parameter) if scope.parameters.first().is_some_and(|declared| &declared.name == parameter)));
    if !bound {
        report(scope.pos, format!("scope `{name}`: a `node: :one` scope binds its parameter with `variables id: :id`"));
    }
    for (present, word) in [(scope.where_.is_some(), "where"), (scope.order.is_some(), "order"), (scope.paginate.is_some(), "paginate"), (scope.total.is_some(), "total"), (!scope.edges.is_empty(), "edge"), (scope.response.is_some(), "response"), (scope.insert_on.is_some(), "insert_on")] {
        if present {
            report(scope.pos, format!("scope `{name}`: `{word}` does not apply to a `node: :one` scope"));
        }
    }
}

pub(super) fn check_node_many(scope: &ScopeDecl, report: &mut impl FnMut(Pos, String)) {
    let name = &scope.name;
    for (present, word) in [(!scope.parameters.is_empty(), "parameter"), (scope.where_.is_some(), "where"), (scope.order.is_some(), "order"), (scope.paginate.is_some(), "paginate"), (scope.total.is_some(), "total"), (!scope.edges.is_empty(), "edge"), (scope.variables.is_some(), "variables"), (scope.insert_on.is_some(), "insert_on")] {
        if present {
            report(scope.pos, format!("scope `{name}`: `{word}` does not apply to a `node: :many` scope"));
        }
    }
    if scope.response.as_ref().is_some_and(|read| read.shape == Some(ResponseShape::One)) {
        report(scope.pos, format!("scope `{name}`: a `node: :many` scope reads a list"));
    }
}

pub(super) fn check_parameters(parameters: &[ParameterDecl], attributes: &HashSet<&str>, standard: bool, report: &mut impl FnMut(Pos, String)) {
    let mut names = HashSet::new();
    for parameter in parameters {
        if !names.insert(parameter.name.as_str()) {
            report(parameter.pos, format!("parameter `{}` is declared twice", parameter.name));
        }
        if standard && attributes.contains(parameter.name.as_str()) {
            report(parameter.pos, format!("parameter `{}` has the name of an attribute", parameter.name));
        }
        if parameter.default.is_some() {
            report(parameter.pos, format!("parameter `{}`: an operation parameter has no `default`", parameter.name));
        }
    }
}

