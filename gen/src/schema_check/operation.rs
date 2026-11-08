use super::*;

impl<'a> Run<'a> {
    pub(super) fn check_mutation_args(&mut self, node: &'a ModelDecl) {
        for operation in &node.operations {
            let Some(name) = operation.mutation.as_ref().map(|found| found.item.as_str()) else { continue };
            let input: Vec<(String, String)> = match (&operation.variables, operation.upload.is_some()) {
                (Some(variables), _) => variables.iter().flat_map(|binding| match &binding.value {
                    VariableValue::Parameter(source) => Some((binding.name.clone(), source.clone())),
                    VariableValue::Constant(_) => None,
                }).collect(),
                (None, true) => operation.parameters.iter().map(|parameter| (parameter.name.clone(), parameter.name.clone())).collect(),
                (None, false) => Vec::new(),
            };
            self.check_args_of(node.file, operation.pos, name, operation.wrap.as_deref(), &operation.parameters, &input);
            self.check_optimistic_maps(node, operation);
            if let Some(batch) = &operation.batch {
                self.check_upload_batch(node, operation, batch, name);
            }
        }
    }

    pub(super) fn check_optimistic_maps(&mut self, node: &ModelDecl, operation: &OperationDecl) {
        let Some((OptimisticDecl::Fields(fields), _)) = &operation.optimistic else { return };
        for (attribute, value, pos) in fields {
            let OptimisticValue::Mapped { from, map } = value else { continue };
            let Some(ParamBase::Named(kind)) = operation.parameters.iter().find(|parameter| parameter.name == *from).map(|parameter| &parameter.ty.base) else { continue };
            let Some(TypeKind::Enum(values)) = self.schema.types.get(kind) else {
                self.fail(node.file, *pos, format!("operation `{}` optimistic `{attribute}`: `{kind}` is not an enum of the schema", operation.name));
                continue;
            };
            let missing: Vec<&str> = values.iter().filter(|value| !map.iter().any(|(key, _)| key == *value)).map(String::as_str).collect();
            let unknown: Vec<&str> = map.iter().map(|(key, _)| key.as_str()).filter(|key| !values.iter().any(|value| value == key)).collect();
            if !missing.is_empty() {
                self.fail(node.file, *pos, format!("operation `{}` optimistic `{attribute}`: the map does not cover {} of `{kind}`", operation.name, missing.join(", ")));
            }
            if !unknown.is_empty() {
                self.fail(node.file, *pos, format!("operation `{}` optimistic `{attribute}`: {} is not a value of `{kind}`", operation.name, unknown.join(", ")));
            }
        }
    }

    pub(super) fn check_args_of(&mut self, file: usize, pos: Pos, mutation: &str, wrap: Option<&str>, args: &[ParameterDecl], input: &[(String, String)]) {
        if args.is_empty() || self.operations.named.is_empty() {
            return;
        }
        let Some(operation) = self.operations.named.get(mutation).filter(|operation| operation.kind == "mutation") else {
            self.fail(file, pos, format!("operation `{mutation}` is not in the operation documents"));
            return;
        };
        let variables = &operation.variables;
        let (owner, accepted) = match wrap {
            None => (format!("operation `{mutation}`"), variables.clone()),
            Some(key) => {
                let Some(wrapper) = variables.get(key) else {
                    self.fail(file, pos, format!("operation `{mutation}` has no variable `${key}`"));
                    return;
                };
                let Some(fields) = self.schema.input_fields(wrapper.name()) else {
                    self.fail(file, pos, format!("variable `${key}` of `{mutation}` is `{}`, not an input type", wrapper.describe()));
                    return;
                };
                (format!("`{}`", wrapper.name()), fields)
            }
        };
        for arg in args {
            let kind = arg.ty.kind_text(arg.null);
            let Some(variable) = input.iter().find(|(_, source)| *source == arg.name).map(|(name, _)| name.as_str()) else { continue };
            let Some(declared) = accepted.get(variable) else {
                self.fail(file, pos, format!("`{variable}` is not an input of {owner}"));
                continue;
            };
            let base = kind.trim_end_matches('?');
            if !self.arg_fits(base, declared) {
                self.fail(file, pos, format!("parameter `{}` of kind `{}` does not fit `{variable}: {}`{}", arg.name, crate::operation::display_kind(&kind), declared.describe(), self.unmapped_hint(declared.name())));
            } else if kind.ends_with('?') && !declared.optional() {
                self.fail(file, pos, format!("parameter `{}` may be nil but `{variable}: {}` is required", arg.name, declared.describe()));
            }
        }
    }

    pub(super) fn check_upload_batch(&mut self, node: &ModelDecl, call: &OperationDecl, batch: &crate::decl::BatchDecl, mutation: &str) {
        let wrap = call.wrap.as_deref();
        let fail = |run: &mut Self, message| run.fail(node.file, call.pos, message);
        let Some(operation) = self.operations.named.get(mutation) else {
            fail(self, "upload batch mutation is missing from the operation documents".into());
            return;
        };
        let accepted = match wrap {
            Some(wrap) => match operation.variables.get(wrap).and_then(|ty| self.schema.input_fields(ty.name())) {
                Some(fields) => fields,
                None => {
                    fail(self, format!("upload batch wrap `{wrap}` must name an input object variable of `{mutation}`"));
                    return;
                }
            },
            None => operation.variables.clone(),
        };
        let items = batch.items.as_str();
        let Some(ty) = accepted.get(items).filter(|ty| ty.is_list()) else {
            fail(self, format!("upload batch `{items}` must be a list of input objects"));
            return;
        };
        let Some(fields) = self.schema.input_fields(ty.name()) else {
            fail(self, format!("upload batch `{items}` must contain input objects"));
            return;
        };
        if fields.get("signedId").is_none_or(|ty| !self.arg_fits("string", ty)) {
            fail(self, "upload batch items require a string signedId".into());
        }
        for (target, source) in &batch.input {
            let kind = node.attributes.iter().find(|field| &field.name == source).map(|field| fit_word(field).0);
            if fields.get(target).is_none_or(|ty| kind.is_none_or(|kind| !self.arg_fits(kind, ty))) {
                fail(self, format!("upload item `{target}` does not accept attribute `{source}`"));
            }
        }
        let single_operation = node.operations.iter().find(|candidate| candidate.name == batch.single);
        let name = single_operation.and_then(|single| single.mutation.as_ref()).map(|found| found.item.as_str());
        let Some(operation) = name.and_then(|name| self.operations.named.get(name)) else {
            fail(self, "single upload mutation is missing from the operation documents".into());
            return;
        };
        let single_fields = match single_operation.and_then(|single| single.wrap.as_deref()) {
            Some(wrap) => match operation.variables.get(wrap).and_then(|ty| self.schema.input_fields(ty.name())) {
                Some(fields) => fields,
                None => {
                    fail(self, format!("single upload wrap `{wrap}` must name an input object variable"));
                    return;
                }
            },
            None => operation.variables.clone(),
        };
        for key in std::iter::once("signedId").chain(batch.input.iter().map(|(target, _)| target.as_str())) {
            if single_fields.get(key).is_none_or(|ty| fields.get(key).is_none_or(|item| item != ty)) {
                fail(self, format!("single upload variable `{key}` must match the batch item field"));
            }
        }
    }

    pub(super) fn custom_fits(&self, named: &str, kind: &str) -> bool {
        matches!(self.schema.types.get(named), Some(TypeKind::Scalar)) && self.scalars.get(named).is_some_and(|mapped| scalars::fits(mapped, kind))
    }

    pub(super) fn unmapped_hint(&self, named: &str) -> String {
        if matches!(self.schema.types.get(named), Some(TypeKind::Scalar)) && !Schema::is_builtin_scalar(named) && !self.scalars.contains_key(named) { format!(": {}", scalars::unmapped(named)) } else { String::new() }
    }

    pub(super) fn arg_fits(&self, kind: &str, declared: &Ty) -> bool {
        let named = declared.name();
        match kind {
            _ if kind.starts_with('[') => {
                let TyKind::List(item) = &declared.kind else { return false };
                let inner = kind.trim_start_matches('[').trim_end_matches(']');
                (!inner.ends_with('?') || item.optional()) && !item.is_list() && self.arg_fits(inner.trim_end_matches('?'), item)
            }
            "ids" => declared.is_list() && (matches!(named, "ID" | "String" | "Int") || self.custom_fits(named, "id")),
            _ if declared.is_list() => false,
            "string" => matches!(named, "String" | "ID") || self.custom_fits(named, "string"),
            "id" => matches!(named, "ID" | "String" | "Int") || self.custom_fits(named, "id"),
            "int" => named == "Int" || self.custom_fits(named, "int"),
            "long" => self.custom_fits(named, "long"),
            "num" => matches!(named, "Float" | "Int") || self.custom_fits(named, "num"),
            "bool" => named == "Boolean" || self.custom_fits(named, "bool"),
            _ => named == kind && matches!(self.schema.types.get(named), Some(TypeKind::Input(_) | TypeKind::Enum(_))),
        }
    }

    pub(super) fn resolve(&mut self, node: &ModelDecl, remote: &ScopeDecl) -> Option<String> {
        let query = remote.query.as_ref()?;
        let name = query.operation.clone();
        let root = if self.operations.named.is_empty() {
            graphql::Root { key: name.clone(), field: name.clone() }
        } else if let Some(root) = self.operations.named.get(&name).filter(|operation| operation.kind == "query").and_then(|operation| operation.root.as_ref()) {
            root.clone()
        } else {
            self.fail(node.file, query.pos, format!("operation `{name}` is not in the operation documents"));
            return None;
        };
        let path = root_path(remote).unwrap_or(&root.key).to_string();
        let (reached, miss) = self.follow_schema_path(node.file, query.pos, "response from", &name, &root, &path)?;
        if let Some(miss) = miss {
            let at = remote.response.as_ref().and_then(|read| read.from.as_ref()).map_or(query.pos, |found| found.pos);
            self.report_miss(Spot { file: node.file, at, label: "response from" }, &name, &path, &[], &path.split('.').collect::<Vec<_>>(), miss);
            return None;
        }
        let paginates = remote.paginate.is_some();
        let lists = remote.node == Some(NodeRead::Many) || remote.response.as_ref().is_some_and(|read| read.shape == Some(crate::decl::ResponseShape::List));
        let pos = remote.paginate.as_ref().map(|paginate| paginate.pos).or_else(|| remote.response.as_ref().map(|read| read.pos)).unwrap_or(query.pos);
        if paginates {
            let element = self.connection_node(node, pos, &reached, remote)?;
            self.check_total(node, remote, &name, &root)?;
            return Some(element);
        }
        if lists != reached.is_list() {
            let expected = if lists { "a list" } else { "one object" };
            self.fail(node.file, pos, format!("scope `{}` expects {expected} but `{path}` returns `{}`", remote.name, reached.describe()));
            return None;
        }
        Some(reached.name().to_string())
    }

    pub(super) fn check_total(&mut self, node: &ModelDecl, remote: &ScopeDecl, operation: &str, root: &graphql::Root) -> Option<()> {
        let Some(total) = &remote.total else { return Some(()) };
        let path = total.item.clone();
        let (reached, miss) = self.follow_schema_path(node.file, total.pos, "total", operation, root, &path)?;
        if reached.name() != "Int" || reached.is_list() || !path.contains('.') {
            self.fail(node.file, total.pos, format!("total `{path}` must be an `Int` but is `{}`", reached.describe()));
            return None;
        }
        if let Some(miss) = miss {
            self.report_miss(Spot { file: node.file, at: total.pos, label: "total" }, operation, &path, &[], &path.split('.').collect::<Vec<_>>(), miss);
            return None;
        }
        Some(())
    }

    pub(super) fn follow_schema_path(&mut self, file: usize, pos: Pos, label: &str, operation: &str, root: &graphql::Root, path: &str) -> Option<(Ty, Option<graphql::Miss>)> {
        let keys: Vec<&str> = path.split('.').collect();
        if keys[0] != root.key {
            self.fail(file, pos, format!("{label} `{path}` must start with the first field `{}` of operation `{operation}`", root.key));
            return None;
        }
        let mut reached = match self.schema.field(&self.schema.query, &root.field) {
            Ok(reached) => reached,
            Err(message) => {
                self.fail(file, pos, message);
                return None;
            }
        };
        if self.schema.is_interface(reached.name()) {
            reached = self.enter_implementation(file, pos, operation, &root.key, reached)?;
        }
        let followed = paths::segments_of(path).and_then(|_| self.follow_quiet(operation, None, &keys));
        let names: Vec<&str> = match &followed {
            Some(Ok(found)) => found.names.clone(),
            _ => keys.clone(),
        };
        for segment in names.into_iter().skip(1) {
            reached = match self.schema.field(reached.name(), segment) {
                Ok(next) if !reached.is_list() => next,
                Ok(_) => {
                    self.fail(file, pos, format!("{label} `{path}` crosses the list `{}`", reached.describe()));
                    return None;
                }
                Err(message) => {
                    self.fail(file, pos, message);
                    return None;
                }
            };
        }
        Some((reached, followed.and_then(Result::err)))
    }

    pub(super) fn enter_implementation(&mut self, file: usize, pos: Pos, operation: &str, key: &str, reached: Ty) -> Option<Ty> {
        let interface = reached.name().to_string();
        let Some(select) = self.operations.named.get(operation).and_then(|found| found.select.fields.iter().find(|field| field.key == key)).map(|field| &field.select) else {
            self.fail(file, pos, format!("`{interface}` root needs the operation documents of `{operation}`"));
            return None;
        };
        let mut types: Vec<&str> = Vec::new();
        let mut bare: Vec<&str> = Vec::new();
        self.operations.each_select(select, None, &mut |inner, on| match on {
            Some(on) if on != interface => types.extend((!types.contains(&on)).then_some(on)),
            _ => bare.extend(inner.fields.iter().map(|field| field.key.as_str())),
        }, &mut Vec::new());
        let problems: Vec<String> = bare.iter().map(|field| format!("field `{field}` of `{key}` is selected outside `... on`, a type that does not match would still give a row")).collect();
        let implementation = match types.as_slice() {
            [one] if self.schema.implementers.get(&interface).is_some_and(|all| all.iter().any(|name| name == one)) => Some(one.to_string()),
            [one] => {
                self.fail(file, pos, format!("`... on {one}` is not an implementation of `{interface}`"));
                None
            }
            [] => {
                self.fail(file, pos, format!("`{key}` of `{operation}` is `{interface}`: select its fields under `... on <Type>`"));
                None
            }
            _ => {
                self.fail(file, pos, format!("`{key}` of `{operation}` selects more than one `... on` type: {}", types.join(", ")));
                None
            }
        };
        for problem in problems {
            self.fail(file, pos, problem);
        }
        let named = |name: String| Ty { kind: TyKind::Named(name), nullable: true, has_default: false };
        implementation.map(|name| Ty { nullable: reached.nullable, ..if reached.is_list() { named(name).into_list() } else { named(name) } })
    }

    pub(super) fn connection_node(&mut self, node: &ModelDecl, pos: Pos, connection: &Ty, remote: &ScopeDecl) -> Option<String> {
        let nodes = self.schema.find_field(connection.name(), "nodes").filter(Ty::is_list);
        let edges = self.schema.find_field(connection.name(), "edges");
        self.edge = edges.as_ref().map(|edges| edges.name().to_string());
        let by_edges = nodes.is_none() || (self.edge_select(remote).is_some() && !self.selects_nodes(remote));
        let found = match (edges, nodes) {
            (Some(edges), _) if by_edges => self.schema.find_field(edges.name(), "node").map(|edge| edge.name().to_string()),
            (_, Some(nodes)) => Some(nodes.name().to_string()),
            _ => None,
        };
        if found.is_none() {
            self.fail(node.file, pos, format!("`paginate` needs `nodes` or `edges {{ node }}` on `{}`", connection.name()));
        }
        found
    }


}
