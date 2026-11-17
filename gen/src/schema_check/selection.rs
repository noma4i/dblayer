use super::*;

impl<'a> Run<'a> {
    pub(super) fn fail(&mut self, file: usize, pos: Pos, message: String) {
        self.found.push(FileError { file: self.files[file].clone(), error: ParseError::new(pos.line, pos.column, message) });
    }

    pub(super) fn check_model(&mut self, node: &'a ModelDecl, nodes: &'a [ModelDecl]) {
        let mut seen = HashSet::new();
        for remote in node.scopes.iter().filter(|scope| scope.is_remote()) {
            self.check_text_defaults(node, remote);
            self.check_scope_variables(node, remote);
        }
        for remote in node.scopes.iter().filter(|scope| scope.is_remote()) {
            self.edge = None;
            let Some(element) = self.resolve(node, remote) else { continue };
            if let Some(edge) = self.edge.take() {
                self.check_edge_fields(node, remote, &edge);
            }
            if !seen.insert(element.clone()) {
                continue;
            }
            let keys = node.associations.iter().map(|association| crate::build::association_key(association, &node.name)).collect();
            let fields: Vec<&AttributeDecl> = node.attributes.iter().collect();
            let mut covered = BTreeSet::new();
            let selects: Vec<&'a Select> = node.scopes.iter().filter(|scope| scope.is_remote()).flat_map(|scope| self.element_select(scope)).collect();
            self.check_fields(node, &fields, &element, &Scope { keys, top: true, ..Scope::inside(node.file, selects.clone()) }, &mut covered, &mut Vec::new());
            self.check_associations(node, &element, selects);
            self.list_unmodeled(node, remote, &element, &covered);
        }
        for scope in node.scopes.iter().filter(|scope| scope.is_remote()) {
            self.check_media(node, scope);
        }
        self.check_mutation_args(node);
        self.check_response_paths(node, nodes);
    }

    pub(super) fn check_edge_fields(&mut self, node: &'a ModelDecl, remote: &'a ScopeDecl, edge: &str) {
        let select = self.edge_select(remote);
        let documented = self.root_select(remote).is_some();
        for field in &remote.edges {
            let name = field.name.as_str();
            if documented && select.is_none() {
                self.fail(node.file, field.pos, format!("attribute `{name}`: the document does not select `edges`"));
                continue;
            }
            let key = field.from.as_ref().map_or(name, |found| found.item.as_str());
            let segments: Vec<&str> = key.split('.').collect();
            let selected = select.map(|select| self.operations.fields_in(select, segments[0], &[]));
            let real = selected.as_ref().and_then(|found| found.first().map(|field| field.name.clone())).unwrap_or_else(|| segments[0].to_string());
            if let Err(message) = self.schema.field(edge, &real) {
                self.fail(node.file, field.pos, format!("attribute `{name}`: {message}"));
                continue;
            }
            if selected.is_some_and(|found| found.is_empty()) {
                self.fail(node.file, field.pos, format!("attribute `{name}`: the document does not select `{}` on the edges", segments[0]));
                continue;
            }
            let selects: Vec<&'a Select> = select.into_iter().collect();
            let Some(reached) = self.walk(node.file, field.pos, edge, &segments, &Scope::inside(node.file, selects), name) else { continue };
            let word = fit_word(field).0;
            if let Some(expected) = exact_word(reached.ty.name()).filter(|expected| *expected != word && !reached.ty.is_list()) {
                self.fail(node.file, field.pos, format!("attribute `{name}` is declared `{word}` but the edge field is `{}`, declare `{expected}`", reached.ty.describe()));
                continue;
            }
            self.check_kind(field, name, &reached.ty, reached.nullable_hop, node.file);
        }
    }

    pub(super) fn edge_select(&self, remote: &ScopeDecl) -> Option<&'a Select> {
        let select = self.root_select(remote)?;
        Some(&select.fields.iter().find(|field| field.name == "edges")?.select)
    }

    pub(super) fn check_text_defaults(&mut self, node: &ModelDecl, remote: &ScopeDecl) {
        for parameter in remote.parameters.iter().filter(|parameter| parameter.default.is_some()) {
            let Some(default) = &parameter.default else { continue };
            let scope = &remote.name;
            let ParamBase::Named(kind) = &parameter.ty.base else { continue };
            let (Some(TypeKind::Enum(values)), Some(value)) = (self.schema.types.get(kind), default.item.as_str()) else { continue };
            if !values.iter().any(|candidate| candidate == value) {
                self.fail(node.file, default.pos, format!("scope `{scope}` default `{value}` is not a value of `{kind}`"));
            }
        }
    }
}
