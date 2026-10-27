use super::*;

impl<'a> Run<'a> {
    pub(super) fn check_media(&mut self, node: &'a ModelDecl, scope: &ScopeDecl) {
        let (Some(marker), Some(tail)) = (&scope.marker, &scope.tail) else { return };
        for (name, pos, automatic) in [(&marker.operation, marker.pos, "upToSequence"), (&tail.operation, tail.pos, tail.after.item.as_str())] {
            let Some(operation) = self.operations.named.get(name).filter(|operation| operation.kind == "query") else {
                self.fail(node.file, pos, format!("marker/tail operation `{name}` is not a query in the documents"));
                continue;
            };
            let integer = |ty: &Ty| !ty.is_list() && matches!(ty.name(), "Int" | "Int64");
            if !operation.variables.get(automatic).is_some_and(integer) {
                self.fail(node.file, pos, format!("operation `{name}` needs integer variable `${automatic}`"));
            }
            for (variable, ty) in &operation.variables {
                if [automatic, "first", "last", "after", "before"].contains(&variable.as_str()) { continue; }
                let mapped = scope.variables.iter().flatten().find(|binding| binding.name == *variable).map(|binding| &binding.value);
                if let Some(VariableValue::Constant(value)) = mapped {
                    let fits = match value {
                        serde_json::Value::String(_) => matches!(ty.name(), "String" | "ID" | "Int64") || matches!(self.schema.types.get(ty.name()), Some(TypeKind::Enum(_))),
                        serde_json::Value::Number(_) => integer(ty) || ty.name() == "Float",
                        serde_json::Value::Bool(_) => ty.name() == "Boolean",
                        serde_json::Value::Null => ty.optional(),
                        _ => ty.is_list(),
                    };
                    if !fits { self.fail(node.file, pos, format!("marker/tail variable `${variable}` has an incompatible constant")); }
                    continue;
                }
                let source = match mapped { Some(VariableValue::Parameter(source)) => source.as_str(), _ => variable.as_str() };
                match scope.parameters.iter().find(|parameter| parameter.name == source) {
                    Some(parameter) if !self.arg_fits(parameter.ty.kind_text(parameter.null).trim_end_matches('?'), ty) || parameter.null && !ty.optional() => {
                        self.fail(node.file, parameter.pos, format!("marker/tail parameter `{source}` does not fit `${variable}: {}`", ty.describe()));
                    }
                    None if !ty.optional() => self.fail(node.file, pos, format!("marker/tail `${variable}` has no scope parameter")),
                    _ => {}
                }
            }
        }
        if let Some(root) = self.operations.named.get(&marker.operation).and_then(|operation| operation.root.clone()) {
            for field in marker.fields.iter().chain([&marker.sequence]) {
                let path = format!("{}.{}", marker.from.item, field.item);
                if self.follow_operation(node.file, field.pos, "marker from", &marker.operation, &path).is_none() { continue; }
                if let Some((ty, _)) = self.follow_schema_path(node.file, field.pos, "marker from", &marker.operation, &root, &path) {
                    if ty.is_list() || ty.nullable || !matches!(ty.name(), "Int" | "Int64") {
                        self.fail(node.file, field.pos, format!("marker `{path}` must be a non-null integer/revision, got {}", ty.describe()));
                    }
                }
            }
        }
        if let Some(root) = self.operations.named.get(&tail.operation).and_then(|operation| operation.root.clone()) {
            let path = root_path(scope).unwrap_or(&root.key);
            self.follow_operation(node.file, tail.pos, "tail response from", &tail.operation, path);
            for field in ["pageInfo.hasNextPage", "pageInfo.endCursor"] {
                self.follow_operation(node.file, tail.pos, "tail response from", &tail.operation, &format!("{path}.{field}"));
            }
            if let Some((ty, _)) = self.follow_schema_path(node.file, tail.pos, "tail response from", &tail.operation, &root, path) {
                if ty.is_list() || self.schema.fields(ty.name()).is_none() { self.fail(node.file, tail.pos, "tail response must be a connection".into()); }
            }
        }
        let mut tail_scope = scope.clone();
        tail_scope.query = Some(crate::decl::QueryDecl { pos: tail.pos, operation: tail.operation.clone() });
        let full_element = self.resolve(node, scope);
        let tail_element = self.resolve(node, &tail_scope);
        if let Some(element) = tail_element {
            if full_element.as_ref() != Some(&element) { self.fail(node.file, tail.pos, "tail and query must return the same model type".into()); }
            if let Some(selected) = self.element_select(&tail_scope) {
                let fields: Vec<_> = node.attributes.iter().collect();
                let keys = node.associations.iter().map(|association| crate::build::association_key(association, &node.name)).collect();
                self.check_fields(node, &fields, &element, &Scope { keys, top: true, ..Scope::inside(node.file, vec![selected]) }, &mut BTreeSet::new(), &mut Vec::new());
            } else { self.fail(node.file, tail.pos, "tail must select model rows".into()); }
        }
    }
}
