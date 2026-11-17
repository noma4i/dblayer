use super::*;

const PAGING: [&str; 4] = ["first", "last", "after", "before"];

struct Document<'d> {
    name: &'d str,
    pos: Pos,
    operation: &'d graphql::Operation,
    automatic: Vec<&'d str>,
    own: bool,
}

impl<'a> Run<'a> {
    pub(super) fn check_scope_variables(&mut self, node: &ModelDecl, scope: &ScopeDecl) {
        let operations = self.operations;
        let query = scope.query.as_ref().expect("a remote scope has a query");
        let cursor = scope.paginate.iter().flat_map(|paginate| paginate.cursor.iter()).map(|cursor| cursor.parameter.as_str());
        let query_automatic: Vec<&str> = if scope.node == Some(NodeRead::Many) { vec!["ids"] } else if scope.paginate.is_some() { PAGING.iter().copied().chain(cursor).collect() } else { Vec::new() };
        let mut sent = vec![(query.operation.as_str(), query.pos, query_automatic, true)];
        if let (Some(marker), Some(tail)) = (&scope.marker, &scope.tail) {
            sent.push((marker.operation.as_str(), marker.pos, PAGING.iter().copied().chain(["upToSequence"]).collect(), false));
            sent.push((tail.operation.as_str(), tail.pos, PAGING.iter().copied().chain([tail.after.item.as_str()]).collect(), false));
        }
        sent.extend(scope.subscriptions.iter().map(|subscription| (subscription.name.as_str(), subscription.pos, Vec::new(), false)));
        let mut documents: Vec<Document> = Vec::new();
        for (name, pos, automatic, own) in sent {
            let Some(operation) = operations.named.get(name) else { continue };
            documents.push(Document { name, pos, operation, automatic, own });
        }
        let Some(first) = documents.first().filter(|document| document.name == query.operation) else { return };
        let mut found: Vec<(Pos, String)> = Vec::new();
        let prefix = |document: &str| format!("model `{}` scope `{}` operation `{document}`: ", node.name, scope.name);
        for binding in scope.variables.iter().flatten() {
            if !documents.iter().any(|document| document.operation.variables.contains_key(&binding.name)) {
                found.push((binding.pos, format!("{}variable `${}` is not a variable of the document", prefix(first.name), binding.name)));
            }
        }
        let linked: Vec<&str> = scope.where_.iter().flat_map(|clause| clause.pairs.iter().map(|(_, parameter)| parameter.item.as_str())).collect();
        let mapped = |parameter: &str| scope.variables.iter().flatten().find(|binding| matches!(&binding.value, VariableValue::Parameter(source) if source == parameter)).map_or(parameter.to_string(), |binding| binding.name.clone());
        for document in &documents {
            let at = prefix(document.name);
            let variables = &document.operation.variables;
            let mut sources: Vec<String> = scope.parameters.iter().map(|parameter| mapped(&parameter.name)).chain(scope.variables.iter().flatten().map(|binding| binding.name.clone())).collect();
            sources.extend(document.automatic.iter().map(|name| name.to_string()));
            let mut missing: Vec<&String> = variables.iter().filter(|(name, ty)| !ty.optional() && !sources.contains(name)).map(|(name, _)| name).collect();
            missing.sort();
            for name in missing {
                found.push((document.pos, format!("{at}variable `${name}` has no source: declare `parameter :{name}` or give it with `variables {name}: ...`")));
            }
            for binding in scope.variables.iter().flatten() {
                let (Some(ty), VariableValue::Constant(value)) = (variables.get(&binding.name), &binding.value) else { continue };
                let integer = !ty.is_list() && matches!(ty.name(), "Int" | "Int64");
                let fits = match value {
                    serde_json::Value::String(_) => matches!(ty.name(), "String" | "ID" | "Int64") || matches!(self.schema.types.get(ty.name()), Some(TypeKind::Enum(_))),
                    serde_json::Value::Number(_) => integer || ty.name() == "Float",
                    serde_json::Value::Bool(_) => ty.name() == "Boolean",
                    serde_json::Value::Null => ty.optional(),
                    _ => ty.is_list(),
                };
                if !fits {
                    found.push((binding.pos, format!("{at}variable `${}` has an incompatible constant", binding.name)));
                }
            }
            for parameter in &scope.parameters {
                let variable = mapped(&parameter.name);
                let kind = parameter.ty.kind_text(parameter.null);
                let kind = kind.trim_end_matches('?');
                let Some(declared) = variables.get(&variable) else {
                    if document.own && !linked.contains(&parameter.name.as_str()) {
                        found.push((parameter.pos, format!("{at}param `{}` is `{kind}` but the document has no variable `${variable}`", parameter.name)));
                    }
                    continue;
                };
                if parameter.null && !declared.optional() {
                    found.push((parameter.pos, format!("{at}param `{}` may be nil but the variable `${variable}` of `{}` is `{}`", parameter.name, document.name, declared.describe())));
                    continue;
                }
                if let ParamBase::Named(named) = &parameter.ty.base {
                    if !matches!(self.schema.types.get(named), Some(TypeKind::Enum(_))) {
                        found.push((parameter.pos, format!("{at}param `{}` names `{named}`, which is not an enum of the schema", parameter.name)));
                        continue;
                    }
                }
                if !self.arg_fits(kind, declared) {
                    found.push((parameter.pos, format!("{at}param `{}` is `{kind}` but the variable `${variable}` of `{}` is `{}`", parameter.name, document.name, declared.describe())));
                }
            }
        }
        found.dedup();
        for (pos, message) in found {
            self.fail(node.file, pos, message);
        }
    }
}
