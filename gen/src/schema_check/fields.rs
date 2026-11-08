use super::*;

impl<'a> Run<'a> {
    pub(super) fn body_of(&self, attribute: &'a AttributeDecl, file: usize) -> Option<(usize, Body<'a>)> {
        match &attribute.block {
            AttributeBlock::Fields(inner) => Some((file, Body::Fields(inner))),
            AttributeBlock::Union { shared, variants } => Some((file, Body::Union(shared.as_ref().map(|shared| shared.item.as_slice()), variants))),
            AttributeBlock::None => match &attribute.ty {
                TypeRef::Named(name) | TypeRef::NamedList(name) => self.declared.get(name.as_str()).copied(),
                _ => None,
            },
        }
    }

    pub(super) fn check_fields(&mut self, node: &'a ModelDecl, attributes: &[&'a AttributeDecl], owner: &str, scope: &Scope<'a>, covered: &mut BTreeSet<String>, stack: &mut Vec<(String, String)>) {
        for attribute in attributes {
            let name = attribute.name.as_str();
            if attribute.ty == TypeRef::File || attribute.local || name.is_empty() {
                continue;
            }
            let Some(path) = self.path_of(node, attribute, name) else { continue };
            let segments: Vec<&str> = path.iter().map(String::as_str).collect();
            let Some(reached) = self.walk(scope.file, attribute.pos, owner, &segments, scope, name) else { continue };
            covered.insert(reached.first.clone());
            self.check_kind(attribute, name, &reached.ty, reached.nullable_hop, scope.file);
            let Some((file, body)) = self.body_of(attribute, scope.file) else { continue };
            match body {
                Body::Union(shared, variants) => {
                    for select in &reached.selects {
                        self.check_union_selection(scope.file, attribute, reached.ty.name(), variants, select);
                    }
                    self.check_union(&UnionAt { node, attribute, name: reached.ty.name(), shared, variants, file, selects: &reached.selects }, stack);
                }
                Body::Fields(children) => {
                    let type_name = match &attribute.ty {
                        TypeRef::Named(type_name) | TypeRef::NamedList(type_name) => type_name.as_str(),
                        _ => name,
                    };
                    let marker = (type_name.to_string(), reached.ty.name().to_string());
                    if !matches!(self.schema.types.get(reached.ty.name()), Some(TypeKind::Fields(_))) || stack.contains(&marker) {
                        continue;
                    }
                    stack.push(marker);
                    let inner: Vec<&AttributeDecl> = children.iter().collect();
                    self.check_fields(node, &inner, reached.ty.name(), &Scope::inside(file, reached.selects), &mut BTreeSet::new(), stack);
                    stack.pop();
                }
            }
        }
    }

    pub(super) fn element_select(&self, remote: &ScopeDecl) -> Option<&'a Select> {
        let mut select = self.root_select(remote)?;
        if remote.paginate.is_some() {
            select = match select.fields.iter().find(|field| field.name == "nodes") {
                Some(nodes) => &nodes.select,
                None => &select.fields.iter().find(|field| field.name == "edges")?.select.fields.iter().find(|field| field.name == "node")?.select,
            };
        }
        Some(select)
    }

    pub(super) fn selects_nodes(&self, remote: &ScopeDecl) -> bool {
        self.root_select(remote).is_some_and(|select| select.fields.iter().any(|field| field.name == "nodes"))
    }

    pub(super) fn root_select(&self, remote: &ScopeDecl) -> Option<&'a Select> {
        let operations: &'a Operations = self.operations;
        let operation = operations.named.get(query_of(remote))?;
        let root = operation.root.as_ref()?;
        let path = root_path(remote).unwrap_or(&root.key);
        let keys: Vec<&str> = path.split('.').collect();
        let Ok(followed) = operations.follow(&[&operation.select], &keys) else { return None };
        followed.selects.first().copied()
    }

    pub(super) fn check_union_selection(&mut self, file: usize, attribute: &AttributeDecl, union: &str, variants: &[VariantDecl], select: &Select) {
        let noun = if self.schema.is_interface(union) { "interface" } else { "union" };
        let mut found: (bool, Vec<&str>) = (false, Vec::new());
        self.operations.each_select(select, None, &mut |inner, on| {
            found.0 |= inner.typename;
            found.1.extend(on.filter(|on| *on != union));
        }, &mut Vec::new());
        let name = attribute.name.as_str();
        if !found.0 {
            self.fail(file, attribute.pos, format!("attribute `{name}`: the selection of `{}` must select `__typename`", attribute.from.as_ref().map_or(name, |found| found.item.as_str())));
        }
        for on in found.1.iter().filter(|on| !variants.iter().any(|variant| variant.type_name == **on)) {
            self.fail(file, attribute.pos, format!("attribute `{name}`: `... on {on}` is not a declared variant of {noun} `{union}`"));
        }
    }

    pub(super) fn check_union(&mut self, at: &UnionAt<'_, 'a>, stack: &mut Vec<(String, String)>) {
        let UnionAt { node, attribute, name: union, shared, variants, file, selects } = *at;
        let interface = self.schema.is_interface(union);
        let members: Vec<String> = match self.schema.types.get(union) {
            Some(TypeKind::Union(members)) => members.clone(),
            Some(TypeKind::Fields(_)) if interface => self.schema.implementers.get(union).cloned().expect("an interface has an implementers entry"),
            _ => return,
        };
        let declared: Vec<&str> = variants.iter().map(|variant| variant.type_name.as_str()).collect();
        for variant in variants {
            if !members.contains(&variant.type_name) {
                let message = if interface { format!("variant `{}` is not an implementation of interface `{union}`", variant.type_name) } else { format!("variant `{}` is not a member of union `{union}`", variant.type_name) };
                self.fail(file, variant.pos, message);
            }
        }
        for member in members.iter().filter(|member| !declared.contains(&member.as_str())) {
            let pos = variants.first().map_or(attribute.pos, |variant| variant.pos);
            let message = if interface { format!("implementation `{member}` of interface `{union}` is not declared") } else { format!("variant `{member}` of union `{union}` is not declared") };
            self.fail(file, pos, message);
        }
        if let Some(shared) = shared {
            self.check_shared(at, shared, &members, interface, stack);
        }
        for variant in variants.iter().filter(|variant| members.contains(&variant.type_name)) {
            let Some(block) = &variant.fields else { continue };
            let marker = (variant.type_name.clone(), union.to_string());
            if stack.contains(&marker) {
                continue;
            }
            stack.push(marker);
            let inner: Vec<&AttributeDecl> = block.iter().collect();
            let apart = declared.iter().filter(|other| **other != variant.type_name).map(|other| other.to_string()).collect();
            self.check_fields(node, &inner, &variant.type_name, &Scope { apart, ..Scope::inside(file, selects.to_vec()) }, &mut BTreeSet::new(), stack);
            stack.pop();
        }
    }

    pub(super) fn check_shared(&mut self, at: &UnionAt<'_, 'a>, shared: &'a [AttributeDecl], members: &[String], interface: bool, stack: &mut Vec<(String, String)>) {
        let UnionAt { node, attribute, name: union, file, selects, .. } = *at;
        let pos = attribute.pos;
        if !interface {
            self.fail(file, pos, format!("`shared` needs an interface: `{union}` is a union"));
            return;
        }
        let fields: Vec<&AttributeDecl> = shared.iter().collect();
        self.check_fields(node, &fields, union, &Scope::inside(file, selects.to_vec()), &mut BTreeSet::new(), stack);
        for field in &fields {
            let key = field.from.as_ref().map_or(field.name.as_str(), |found| found.item.as_str()).split('.').next().unwrap_or_default();
            for member in members {
                let apart: Vec<String> = members.iter().filter(|other| *other != member).cloned().collect();
                let name = selects.iter().find_map(|select| self.operations.fields_in(select, key, &apart).first().map(|found| found.name.as_str())).unwrap_or(key);
                if self.schema.field(member, name).is_err() {
                    self.fail(file, field.pos, format!("implementation `{member}` of interface `{union}` has no field `{name}`"));
                }
            }
        }
    }

    pub(super) fn path_of(&mut self, node: &ModelDecl, attribute: &AttributeDecl, name: &str) -> Option<Vec<String>> {
        if let Some(parent) = &attribute.id_from {
            let association = node.associations.iter().find(|candidate| matches!(candidate.kind, AssociationKind::BelongsTo | AssociationKind::HasOne) && candidate.name == *parent);
            let base = association.and_then(|association| association.from.as_deref()).unwrap_or(parent);
            return Some(base.split('.').map(str::to_string).chain(["id".to_string()]).collect());
        }
        Some(attribute.from.as_ref().map_or(name, |found| found.item.as_str()).split('.').map(str::to_string).collect())
    }

    pub(super) fn walk(&mut self, file: usize, pos: Pos, owner: &str, segments: &[&str], scope: &Scope<'a>, name: &str) -> Option<Walked<'a>> {
        let operations: &'a Operations = self.operations;
        let mut selects = scope.selects.clone();
        let mut first = String::new();
        let mut current = owner.to_string();
        let mut nullable_hop = false;
        let mut crossed = false;
        for (index, key) in segments.iter().enumerate() {
            let selected: Vec<&'a graphql::SelectedField> = selects.iter().flat_map(|inside| operations.fields_in(inside, key, &scope.apart)).collect();
            let segment = selected.first().map_or(*key, |field| field.name.as_str());
            selects = selected.iter().map(|field| &field.select).collect();
            if index == 0 {
                first = segment.to_string();
            }
            let reached = match self.schema.field(&current, segment) {
                Ok(reached) => reached,
                Err(_) if scope.top && scope.keys.contains(name) => return None,
                Err(message) => {
                    self.fail(file, pos, format!("attribute `{name}`: {message}"));
                    return None;
                }
            };
            if index + 1 == segments.len() {
                return Some(Walked { ty: if crossed && !reached.is_list() { reached.into_list() } else { reached }, nullable_hop, first, selects });
            }
            if reached.is_list() && !scope.lists {
                self.fail(file, pos, format!("attribute `{name}`: path `{}` crosses the list `{segment}`", segments.join(".")));
                return None;
            }
            crossed |= reached.is_list();
            nullable_hop |= reached.nullable;
            current = reached.name().to_string();
        }
        None
    }

    pub(super) fn check_kind(&mut self, attribute: &AttributeDecl, name: &str, reached: &Ty, nullable_hop: bool, file: usize) {
        let (word, _) = fit_word(attribute);
        if word == "raw" {
            return;
        }
        let mut problems = Vec::new();
        if name != "id" && (reached.nullable || nullable_hop) && !(attribute.null || attribute.optional || attribute.default.is_some()) {
            problems.push(format!("is nullable in the schema (`{}`), declare `null: true`", reached.describe()));
        }
        let wants_list = matches!(word, "list" | "array");
        if wants_list != reached.is_list() {
            let schema_shape = if reached.is_list() { "a list" } else { "not a list" };
            problems.push(format!("`{word}` does not fit `{}`, which is {schema_shape}", reached.describe()));
        } else if !self.named_fits(attribute, reached.name()) {
            problems.push(format!("`{word}` does not fit the schema type `{}`{}", reached.describe(), self.unmapped_hint(reached.name())));
        }
        if matches!(attribute.ty, TypeRef::Enum | TypeRef::List(ListElement::Enum)) {
            problems.extend(self.enum_problems(attribute, reached.name()));
        }
        if attribute.is_union() && !matches!(self.schema.types.get(reached.name()), Some(TypeKind::Union(_))) && !self.schema.is_interface(reached.name()) {
            problems.push(format!("is not a union: schema type `{}` is not one", reached.name()));
        }
        for problem in problems {
            self.fail(file, attribute.pos, format!("attribute `{name}` {problem}"));
        }
    }

    pub(super) fn named_fits(&self, attribute: &AttributeDecl, named: &str) -> bool {
        let kind = self.schema.types.get(named);
        let builtin = Schema::is_builtin_scalar(named);
        let structured = matches!(kind, Some(TypeKind::Fields(_) | TypeKind::Union(_))) || self.custom_fits(named, "object");
        let (word, of) = fit_word(attribute);
        let word = if word == "list" { of.expect("a list names its element kind") } else { word };
        match word {
            "string" => matches!(named, "String" | "ID") || self.custom_fits(named, "string"),
            "id" => matches!(named, "ID" | "String" | "Int") || self.custom_fits(named, "id"),
            "int" => named == "Int" || self.custom_fits(named, "int"),
            "num" => matches!(named, "Float" | "Int") || self.custom_fits(named, "num"),
            "bool" => named == "Boolean" || self.custom_fits(named, "bool"),
            "date" => named == "String" || self.custom_fits(named, "date"),
            "enum" => named == "String" || matches!(kind, Some(TypeKind::Enum(_))),
            "object" => structured,
            "union" => true,
            "array" => structured || builtin,
            _ => true,
        }
    }

    pub(super) fn enum_problems(&self, attribute: &AttributeDecl, named: &str) -> Vec<String> {
        let Some(TypeKind::Enum(known)) = self.schema.types.get(named) else { return Vec::new() };
        let Some(values) = &attribute.values else { return Vec::new() };
        let missing: Vec<&str> = values.iter().map(String::as_str).filter(|value| !known.iter().any(|candidate| candidate == value)).collect();
        if missing.is_empty() { Vec::new() } else { vec![format!("lists values {} that enum `{named}` does not have", missing.join(", "))] }
    }

    pub(super) fn check_associations(&mut self, node: &ModelDecl, element: &str, selects: Vec<&'a Select>) {
        for association in &node.associations {
            let Some(path) = &association.from else { continue };
            let segments: Vec<&str> = path.split('.').collect();
            let Some(Walked { ty: reached, .. }) = self.walk(node.file, association.pos, element, &segments, &Scope { lists: true, ..Scope::inside(node.file, selects.clone()) }, &association.name) else { continue };
            let one = matches!(association.kind, AssociationKind::BelongsTo | AssociationKind::HasOne);
            if one == reached.is_list() {
                let expected = if one { "one object" } else { "a list" };
                self.fail(node.file, association.pos, format!("`{}` `{}` expects {expected} but `{path}` is `{}`", association.kind.word(), association.name, reached.describe()));
            }
        }
    }

    pub(super) fn list_unmodeled(&mut self, node: &ModelDecl, remote: &ScopeDecl, element: &str, covered: &BTreeSet<String>) {
        let Some(fields) = self.schema.fields(element) else { return };
        let missing: BTreeSet<&str> = fields.keys().map(String::as_str).filter(|name| !covered.contains(*name) && !name.starts_with("__")).collect();
        if !missing.is_empty() {
            self.notes.push(format!("{}: model `{}` scope `{}` does not use {} fields of `{element}`: {}", self.files[node.file], node.name, remote.name, missing.len(), missing.into_iter().collect::<Vec<_>>().join(", ")));
        }
    }
}
