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
        }
        if let Some(root) = self.operations.named.get(&marker.operation).and_then(|operation| operation.root.clone()) {
            for field in marker.fields.iter().chain([&marker.sequence]) {
                let path = format!("{}.{}", marker.from.item, field.item);
                if self.follow_operation(Spot { file: node.file, at: field.pos, label: "marker from" }, &marker.operation, &path).is_none() { continue; }
                if let Some((ty, _)) = self.follow_schema_path(node.file, field.pos, "marker from", &marker.operation, &root, &path) {
                    if ty.is_list() || ty.nullable || !matches!(ty.name(), "Int" | "Int64") {
                        self.fail(node.file, field.pos, format!("marker `{path}` must be a non-null integer/revision, got {}", ty.describe()));
                    }
                }
            }
        }
        if let Some(root) = self.operations.named.get(&tail.operation).and_then(|operation| operation.root.clone()) {
            let path = root_path(scope).unwrap_or(&root.key);
            self.follow_operation(Spot { file: node.file, at: tail.pos, label: "tail response from" }, &tail.operation, path);
            for field in ["pageInfo.hasNextPage", "pageInfo.endCursor"] {
                self.follow_operation(Spot { file: node.file, at: tail.pos, label: "tail response from" }, &tail.operation, &format!("{path}.{field}"));
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
