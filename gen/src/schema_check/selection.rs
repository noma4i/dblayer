use super::*;

impl<'a> Run<'a> {
    pub(super) fn fail(&mut self, file: usize, pos: Pos, message: String) {
        self.found.push(FileError { file: self.files[file].clone(), error: ParseError::new(pos.line, pos.column, message) });
    }

    pub(super) fn check_model(&mut self, node: &'a ModelDecl) {
        let mut seen = HashSet::new();
        for remote in node.scopes.iter().filter(|scope| scope.is_remote()) {
            self.check_text_types(node, remote);
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
            self.check_subscription_args(node, scope);
        }
        self.check_mutation_args(node);
        self.check_response_paths(node);
    }

    pub(super) fn check_subscription_args(&mut self, node: &ModelDecl, scope: &ScopeDecl) {
        for subscription in &scope.subscriptions {
            let Some(operation) = self.operations.named.get(&subscription.name) else { continue };
            let name = &subscription.name;
            for (variable, declared) in &operation.variables {
                let parameter = scope.parameters.iter().find(|parameter| {
                    let renamed = scope.variables.iter().flatten().find(|binding| matches!(&binding.value, VariableValue::Parameter(source) if *source == parameter.name));
                    renamed.map_or(parameter.name.as_str(), |binding| binding.name.as_str()) == variable
                });
                let Some(parameter) = parameter else {
                    if !declared.optional() {
                        self.fail(node.file, subscription.pos, format!("scope `{}` subscription `{name}`: argument `${variable}` has no scope parameter, declare `parameter :{variable}` or make the argument optional", scope.name));
                    }
                    continue;
                };
                let accepted: &[&str] = match &parameter.ty.base {
                    ParamBase::Bool => &["Boolean"],
                    ParamBase::Int => &["Int"],
                    ParamBase::String | ParamBase::Id => &["String", "ID"],
                    ParamBase::Named(kind) => &[kind.as_str()],
                    _ => continue,
                };
                if declared.is_list() != parameter.ty.list || !accepted.contains(&declared.name()) {
                    self.fail(node.file, subscription.pos, format!("scope `{}` subscription `{name}`: argument `${variable}` is `{}` but scope parameter `{}` is {}", scope.name, declared.describe(), parameter.name, accepted.join(" or ")));
                }
            }
        }
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

    pub(super) fn check_text_types(&mut self, node: &ModelDecl, remote: &ScopeDecl) {
        self.check_text_defaults(node, remote);
        let document = query_of(remote);
        let linked: Vec<&str> = remote.where_.iter().flat_map(|clause| clause.pairs.iter().map(|(_, parameter)| parameter.item.as_str())).collect();
        let mapped = |param: &str| {
            let renamed = remote.variables.iter().flatten().find(|binding| matches!(&binding.value, VariableValue::Parameter(source) if source == param));
            renamed.map_or(param.to_string(), |binding| binding.name.clone())
        };
        let Some(operation) = self.operations.named.get(document) else { return };
        for parameter in remote.parameters.iter().filter(|parameter| !linked.contains(&parameter.name.as_str()) && !parameter.ty.list) {
            let kind = match &parameter.ty.base {
                ParamBase::Bool => "Boolean".to_string(),
                ParamBase::Int => "Int".to_string(),
                ParamBase::Named(kind) => {
                    if !matches!(self.schema.types.get(kind), Some(TypeKind::Enum(_))) {
                        self.fail(node.file, parameter.pos, format!("scope `{}` param `{}` names `{kind}`, which is not an enum of the schema", remote.name, parameter.name));
                        continue;
                    }
                    kind.clone()
                }
                _ => continue,
            };
            let variable = mapped(&parameter.name);
            match operation.variables.get(&variable) {
                None => self.fail(node.file, parameter.pos, format!("scope `{}` param `{}` is `{kind}` but operation `{document}` has no variable `${variable}`", remote.name, parameter.name)),
                Some(declared) if declared.is_list() || declared.name() != kind => self.fail(node.file, parameter.pos, format!("scope `{}` param `{}` is `{kind}` but the variable `${variable}` of `{document}` is `{}`", remote.name, parameter.name, declared.describe())),
                Some(_) => {}
            }
        }
    }


}
