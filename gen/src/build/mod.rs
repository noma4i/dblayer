use dblayer::schema::{
    ActionDeclaration, ActionLanding, AssociationKind, AssociationSpec, Coverage, CursorSpec, Dependent, EphemeralSpec, FieldKind, FieldSpec, MergeRule, ModelSpec, Newer, NodeRead,
    PageDirection, CopyValue, ReceiverCopy, RelationSpec, RemoteShape, RemoteSpec, RootShape, ScopeEffect, ScopeEffectKind, Sort, SortField, SubscriptionAction, SubscriptionApply, UpdatesSpec, VariableSource, NODE_PARAM, EffectSource,
};
use dblayer::value::ScalarKind;
use serde_json::{json, Map, Value as Json};

use crate::decl::{
    self, AssociationDecl, AttributeBlock, AttributeDecl, CursorType, Effect, InsertOn, ListElement, ModelDecl, OperationDecl, OperationKind, OptimisticDecl, OptimisticElement,
    OptimisticValue, PageDirectionDecl, ParamBase, ResponseDecl, ResponseShape, ScopeDecl, SourceDecl, SubscribeDecl, TypeRef, VariableValue,
};

const DEFAULT_FRESHNESS: &str = "5.minutes";

pub fn build(nodes: &[ModelDecl]) -> Vec<ModelSpec> {
    let mut models: Vec<_> = nodes.iter().map(build_model).collect();
    let indexes: Vec<_> = models.iter().flat_map(|model| model.associations.iter().filter(|association| association.kind != AssociationKind::References).map(move |association| {
        let owner = if association.kind == AssociationKind::BelongsTo { model.key.to_string() } else { association.target.to_string() };
        (owner, association.foreign_key.clone())
    })).collect();
    for model in &mut models {
        for field in model.fields.iter_mut().filter(|field| field.name != "id") {
            if indexes.iter().any(|(owner, key)| owner == model.key.as_str() && key == &field.name)
                || model.relations.iter().any(|relation| relation.by_columns.contains(&field.name)
                    || matches!(&relation.sort, Sort::Fields(fields) if fields.iter().any(|sort| sort.column == field.name))) {
                field.indexed = true;
            }
        }
    }
    let unions = union_names(&models);
    models.iter_mut().flat_map(|model| model.fields.iter_mut()).for_each(|field| retag_references(field, &unions));
    models
}

fn union_names(models: &[ModelSpec]) -> Vec<String> {
    fn walk(field: &FieldSpec, out: &mut Vec<String>) {
        if let (FieldKind::Union | FieldKind::UnionArray, Some(name)) = (&field.kind, &field.app_type) {
            if !field.fields.is_empty() { out.push(name.clone()); }
        }
        field.fields.iter().chain(field.shared.iter()).for_each(|inner| walk(inner, out));
    }
    let mut found = Vec::new();
    models.iter().flat_map(|model| model.fields.iter()).for_each(|field| walk(field, &mut found));
    found
}

fn retag_references(field: &mut FieldSpec, unions: &[String]) {
    if field.fields.is_empty() && field.app_type.as_ref().is_some_and(|name| unions.contains(name)) {
        match field.kind {
            FieldKind::Object => field.kind = FieldKind::Union,
            FieldKind::Array => field.kind = FieldKind::UnionArray,
            _ => {}
        }
    }
    field.fields.iter_mut().chain(field.shared.iter_mut()).for_each(|inner| retag_references(inner, unions));
}

fn build_model(node: &ModelDecl) -> ModelSpec {
    let mut model = ModelSpec::new(&node.name, vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    model.storage = node.storage;
    model.expire_after = node.expires_in.as_ref().map(|found| found.item.clone());
    for attribute in &node.attributes {
        let field = build_field(attribute, &node.name);
        if field.name == "id" {
            model.fields.retain(|candidate| candidate.name != "id");
        }
        model.fields.push(field);
    }
    for search in &node.search {
        if let Some(field) = model.fields.iter_mut().find(|field| field.name == search.item) {
            field.searchable = true;
        }
    }
    model.indexes = node.indexes.iter().map(|index| index.columns.clone()).collect();
    model.ephemeral = node.ephemeral.iter().flat_map(|group| &group.attributes).map(|attribute| EphemeralSpec { field: build_field(attribute, &node.name), reset_after: attribute.reset_after.clone() }).collect();
    model.associations = node.associations.iter().map(|association| build_association(association, &node.name)).collect();
    model.relations = node.scopes.iter().map(build_scope).collect();
    model.listens = node.subscriptions.iter().map(updates_spec).collect();
    model.removed_by = node.removed_by.iter().map(|removal| dblayer::schema::RemovedBy { model: removal.model.clone(), matches: removal.matches.iter().cloned().collect() }).collect();
    model.change_feed = node.changes.first().map(|feed| dblayer::schema::ChangeFeed {
        subscription: feed.subscription.clone(),
        root: feed.from.item.clone(),
        area: feed.area.item.clone(),
        kind: feed.kind.item.clone(),
        id: feed.id.item.clone(),
    });
    model.server_revision = node.server_revision.clone();
    model.changes = node.changed_by.iter().map(|change| dblayer::schema::ChangeRead {
        area: change.area.clone(),
        relation: change.via.clone(),
        parameter: change.parameter.clone(),
        removes_on: change.removes_on.clone().unwrap_or_default(),
        observed: change.via == "observed",
        id_of: change.id_of.as_ref().map(|(model, field)| dblayer::schema::ChangeIdOf { model: model.clone(), field: field.clone() }),
        payload: change.payload.clone(),
    }).collect();
    model.actions = node.operations.iter().map(|operation| build_operation(operation, &node.name)).collect();
    if model.field("id").is_none() {
        model.fields.push(FieldSpec::scalar("id", ScalarKind::Id));
    }
    model
}

fn build_field(attribute: &AttributeDecl, owner: &str) -> FieldSpec {
    let name = attribute.name.as_str();
    let union = attribute.is_union();
    let mut field = match &attribute.ty {
        TypeRef::Scalar(kind) => FieldSpec::scalar(name, *kind),
        TypeRef::Enum => FieldSpec::enumeration(name, &attribute.values.iter().flatten().map(String::as_str).collect::<Vec<_>>()),
        TypeRef::Json => FieldSpec::raw(name),
        TypeRef::File => FieldSpec::file(name),
        TypeRef::Named(_) if union => FieldSpec::union(name, "", Vec::new()),
        TypeRef::Named(_) => FieldSpec::object(name),
        TypeRef::NamedList(_) if union => FieldSpec::union_array(name, "", Vec::new()),
        TypeRef::NamedList(_) => FieldSpec::array(name),
        TypeRef::List(ListElement::Scalar(kind)) => FieldSpec::list(name, *kind),
        TypeRef::List(ListElement::Enum) => {
            let mut list = FieldSpec::list(name, ScalarKind::Str);
            list.values = attribute.values.clone().unwrap_or_default();
            list
        }
    };
    field.nullable = attribute.null;
    field.optional = attribute.optional;
    field.parse_json = attribute.parse_json;
    field.id_from_parent = attribute.id_from.clone();
    field.link = attribute.link.as_ref().map(|(model, id)| dblayer::schema::FieldLink { model: model.clone(), id: id.clone() });
    field.app_type = match &attribute.ty {
        TypeRef::Named(type_name) | TypeRef::NamedList(type_name) => Some(type_name.clone()),
        _ => attribute.as_name.clone(),
    };
    if (matches!(field.kind, FieldKind::Enum { .. }) || !field.values.is_empty()) && field.app_type.is_none() {
        field.app_type = Some(format!("{owner}{}", camel(name)));
    }
    match &attribute.block {
        AttributeBlock::None => {}
        AttributeBlock::Fields(children) => {
            let type_name = field.app_type.clone().unwrap_or_else(|| format!("{owner}{}", camel(name)));
            field.fields = children.iter().map(|child| build_field(child, &type_name)).collect();
        }
        AttributeBlock::Union { shared, variants } => {
            let type_name = field.app_type.clone().unwrap_or_else(|| format!("{owner}{}", camel(name)));
            field.fields = variants.iter().zip(crate::unions::cases_of(variants)).map(|(variant, case)| {
                let fields = variant.fields.iter().flatten().map(|child| build_field(child, &variant.type_name)).collect();
                FieldSpec::variant(&case, &variant.type_name, fields)
            }).collect();
            field.shared = shared.iter().flat_map(|shared| shared.item.iter()).map(|child| build_field(child, &type_name)).collect();
        }
    }
    field.from = attribute.from.as_ref().map(|found| found.item.clone());
    field.default = attribute.default.as_ref().map(|found| found.item.clone());
    field.merge = attribute.merge.as_ref().map(|found| found.item.clone());
    if attribute.local {
        field.merge = Some(MergeRule::Local);
    }
    field
}

pub(crate) fn camel(name: &str) -> String {
    name.split('_').filter(|part| !part.is_empty()).map(|part| {
        let mut chars = part.chars();
        chars.next().map(|first| first.to_uppercase().collect::<String>() + chars.as_str()).unwrap_or_default()
    }).collect()
}

fn lower(name: &str) -> String {
    let mut chars = name.chars();
    chars.next().map(|first| first.to_lowercase().collect::<String>() + chars.as_str()).unwrap_or_default()
}

pub(crate) fn association_key(association: &AssociationDecl, owner: &str) -> String {
    association.foreign_key.clone().unwrap_or_else(|| match association.kind {
        decl::AssociationKind::BelongsTo => format!("{}Id", association.name),
        decl::AssociationKind::References => association.name.clone(),
        _ => format!("{}Id", lower(owner)),
    })
}

pub(crate) fn association_target(association: &AssociationDecl) -> String {
    association.class_name.clone().unwrap_or_else(|| {
        let singular = match association.kind {
            decl::AssociationKind::HasMany | decl::AssociationKind::References => association.name.strip_suffix('s').unwrap_or(&association.name),
            _ => &association.name,
        };
        camel(singular)
    })
}

fn build_association(association: &AssociationDecl, owner: &str) -> AssociationSpec {
    let kind = match association.kind {
        decl::AssociationKind::BelongsTo => AssociationKind::BelongsTo,
        decl::AssociationKind::HasMany => AssociationKind::HasMany,
        decl::AssociationKind::HasOne => AssociationKind::HasOne,
        decl::AssociationKind::References => AssociationKind::References,
    };
    let mut built = AssociationSpec::new(&association.name, kind, &association_target(association), &association_key(association, owner));
    built.touches_parent = association.touch;
    built.sets = association.sets.iter().cloned().collect();
    built.newer = association.newer.iter().map(|(child, parent)| Newer { child: child.clone(), parent: parent.clone() }).collect();
    built.from = association.from.clone();
    built.dependent = association.dependent.then_some(Dependent::Destroy);
    built
}

pub(crate) fn scope_is_singleton(scope: &ScopeDecl) -> bool {
    build_scope(scope).is_singleton()
}

fn build_scope(scope: &ScopeDecl) -> RelationSpec {
    let columns: Vec<&str> = scope.where_.iter().flat_map(|clause| clause.pairs.iter().map(|(column, _)| column.item.as_str())).collect();
    let mut relation = RelationSpec::new(&scope.name, &columns);
    relation.updates = scope.subscriptions.iter().map(updates_spec).collect();
    if let Some(order) = &scope.order {
        relation.sort = Sort::Fields(order.fields.iter().map(|(column, direction, _)| SortField { column: column.clone(), direction: *direction }).collect());
    }
    relation.node = scope.node;
    relation.row_rules = scope.row_rules.iter().map(|(rule, _)| rule.clone()).collect();
    relation.membership = scope.membership.iter().map(|(parameter, rule, _)| (parameter.clone(), rule.clone())).collect();
    if scope.node == Some(NodeRead::One) {
        relation.text_params.push(NODE_PARAM.to_string());
    } else {
        for parameter in scope.parameters.iter().filter(|parameter| !columns.contains(&parameter.name.as_str())) {
            relation.text_params.push(parameter.name.clone());
            match (&parameter.ty.base, &parameter.default) {
                (ParamBase::Int, _) => { relation.scalar_types.insert(parameter.name.clone(), ScalarKind::Int); }
                (ParamBase::Bool, _) => { relation.scalar_types.insert(parameter.name.clone(), ScalarKind::Bool); }
                (ParamBase::Named(kind), _) => { relation.text_types.insert(parameter.name.clone(), kind.clone()); }
                _ => {}
            }
            if let Some(Json::String(value)) = parameter.default.as_ref().map(|found| &found.item) {
                relation.text_defaults.insert(parameter.name.clone(), value.clone());
            }
        }
    }
    relation.is_global = columns.is_empty() && relation.text_params.is_empty();
    if let Some(insert) = &scope.insert_on {
        relation.joins_created = insert.item.contains(&InsertOn::Create);
        relation.joins_events = insert.item.contains(&InsertOn::Event);
        relation.joins_node = insert.item.contains(&InsertOn::Node);
    }
    relation.order_stable = scope.stable_order.is_some();
    relation.refresh_on_foreground = scope.refresh_on_foreground.is_some();
    relation.members_complete = scope.members_complete;
    if let (Some(marker), Some(tail)) = (&scope.marker, &scope.tail) {
        let column = |name: &str| scope.where_.iter().flat_map(|clause| &clause.pairs).find(|(_, linked)| linked.item == name).map_or_else(|| name.to_string(), |(column, _)| column.item.clone());
        let mut variables: std::collections::BTreeMap<String, VariableSource> = scope.parameters.iter().map(|param| (param.name.clone(), VariableSource::Param { name: column(&param.name) })).collect();
        for binding in scope.variables.iter().flatten() {
            let source = match &binding.value {
                VariableValue::Parameter(name) => VariableSource::Param { name: column(name) },
                VariableValue::Constant(value) => VariableSource::Constant { value: value.clone() },
            };
            variables.insert(binding.name.clone(), source);
        }
        relation.media = Some(dblayer::schema::MediaSpec { marker: marker.operation.clone(), from: marker.from.item.clone(), fields: marker.fields.iter().map(|field| field.item.clone()).collect(), sequence: marker.sequence.item.clone(), tail: tail.operation.clone(), after: tail.after.item.clone(), marker_variables: variables.clone(), tail_variables: variables });
    }
    relation.preload_limit = scope.preload.as_ref().and_then(|preload| preload.limit);
    relation.preload_order = scope.preload.iter().flat_map(|preload| &preload.values).map(|(parameter, _)| {
        scope.where_.iter().flat_map(|clause| &clause.pairs).find(|(_, linked)| linked.item == parameter.item).map_or(parameter.item.clone(), |(column, _)| column.item.clone())
    }).collect();
    relation.preload = scope.preload.as_ref().map(|preload| preload.values.iter().map(|(parameter, values)| {
        let column = scope.where_.iter().flat_map(|clause| &clause.pairs).find(|(_, linked)| linked.item == parameter.item).map_or(parameter.item.clone(), |(column, _)| column.item.clone());
        let choice = match values {
            crate::decl::PreloadChoice::Listed(values) => dblayer::schema::PreloadChoice::Listed(values.iter().map(|value| value.item.clone()).collect()),
            crate::decl::PreloadChoice::RowsOf(source) => {
                let (model, relation) = source.item.split_once('.').unwrap_or((&source.item, ""));
                dblayer::schema::PreloadChoice::RowsOf { from: dblayer::schema::PreloadSource { model: model.to_string(), relation: relation.to_string() } }
            }
        };
        (column, choice)
    }).collect());
    if scope.is_remote() {
        relation.remote = Some(build_remote(scope));
        relation.edge_fields = scope.edges.iter().map(|edge| build_field(edge, "")).collect();
    }
    relation
}

fn build_remote(scope: &ScopeDecl) -> RemoteSpec {
    let query = scope.query.as_ref().expect("a remote scope has a query");
    let listed = scope.node == Some(NodeRead::Many) || scope.response.as_ref().is_some_and(|read| read.shape == Some(ResponseShape::List));
    let shape = match (&scope.paginate, listed) {
        (Some(paginate), _) => RemoteShape::Connection {
            direction: if paginate.direction == PageDirectionDecl::Backward { PageDirection::Backward } else { PageDirection::Forward },
            coverage: Coverage::Page,
            cursor: paginate.cursor.as_ref().map(|cursor| CursorSpec { variable: cursor.parameter.clone(), from: cursor.from.item.clone(), numeric: cursor.ty == CursorType::Int }),
            max_pages: paginate.max_pages.as_ref().map(|pages| pages.item),
        },
        (None, true) => RemoteShape::List { coverage: Coverage::Complete },
        (None, false) => RemoteShape::Single,
    };
    let node_parameter = scope.parameters.first().filter(|_| scope.node == Some(NodeRead::One)).map(|parameter| parameter.name.as_str());
    let variables = scope.variables.iter().flatten().map(|binding| {
        let source = match &binding.value {
            VariableValue::Parameter(name) if Some(name.as_str()) == node_parameter => VariableSource::Param { name: NODE_PARAM.to_string() },
            VariableValue::Parameter(name) => VariableSource::Param { name: name.clone() },
            VariableValue::Constant(value) => VariableSource::Constant { value: value.clone() },
        };
        (binding.name.clone(), source)
    }).collect();
    RemoteSpec {
        operation: query.operation.clone(),
        selects_page_info: matches!(shape, RemoteShape::Connection { .. }),
        shape,
        stale_time: scope.freshness.as_ref().map_or(DEFAULT_FRESHNESS.to_string(), |fresh| fresh.expires_in.item.clone()),
        empty_stale_time: scope.freshness.as_ref().and_then(|fresh| fresh.empty_in.as_ref()).map(|found| found.item.clone()),
        page_size: scope.paginate.as_ref().map(|paginate| paginate.size),
        variables,
        root: scope.response.as_ref().and_then(|read| read.from.as_ref()).map(|found| found.item.clone()),
        total_from: scope.total.as_ref().map(|found| found.item.clone()),
    }
}

fn updates_spec(update: &SubscribeDecl) -> UpdatesSpec {
    let apply = &update.apply;
    let matches = apply.matches.iter().map(|(path, value, _)| (path.clone(), value.clone())).collect();
    let action = match apply.effect {
        Effect::Upsert => Some(SubscriptionAction::Upsert),
        Effect::Delete => Some(SubscriptionAction::Delete),
        Effect::Ephemeral => Some(SubscriptionAction::Ephemeral { fields: apply.fields.iter().map(|(name, path, _)| (name.clone(), path.clone())).collect() }),
        Effect::Reread => Some(SubscriptionAction::Reread { via: apply.via.clone().unwrap_or_default() }),
        Effect::Ignore => Some(SubscriptionAction::Ignore),
    };
    UpdatesSpec {
        subscription: update.name.clone(),
        deleted_by: apply.delete_if.as_ref().map(|found| found.item.clone()),
        deleted_unless: apply.delete_unless.as_ref().map(|found| found.item.clone()),
        set: apply.values.iter().map(|(name, value, _)| (name.clone(), value.clone())).collect::<Map<_, _>>(),
        removes: apply.effect == Effect::Delete,
        debounce: update.debounce.clone(),
        root: update.from.as_ref().map(|found| found.item.clone()),
        id: update.id.iter().map(|found| found.item.clone()).collect(),
        apply: action.map(|action| SubscriptionApply { action, matches }),
    }
}

fn build_operation(operation: &OperationDecl, owner: &str) -> ActionDeclaration {
    let mut action = match operation.kind {
        OperationKind::Create => ActionDeclaration::new("create", RootShape::Upsert),
        OperationKind::Update => ActionDeclaration::new("update", RootShape::Upsert),
        OperationKind::Destroy => ActionDeclaration::new("destroy", RootShape::Destroy),
        OperationKind::Custom => ActionDeclaration::new(&operation.name, RootShape::Upsert),
    };
    if operation.kind == OperationKind::Create {
        action.has_optimistic = true;
        action.has_temp_row_retention = true;
    }
    action.mutation = operation.mutation.as_ref().map(|found| found.item.clone());
    action.upload = operation.upload.is_some();
    action.on_failure = operation.on_failure.as_ref().map(|found| found.item).unwrap_or_default();
    match &operation.response {
        Some(ResponseDecl::Ignore) => action.ignore_response = true,
        Some(ResponseDecl::Read(read)) => action.root = read.from.as_ref().map(|found| found.item.clone()),
        None => {}
    }
    action.wrap = operation.wrap.clone();
    action.local = operation.local.clone().unwrap_or_default();
    action.local_from = operation.local_from.iter().map(|(name, source, _)| (name.clone(), build_source(source))).collect();
    action.correlate = operation.match_by.iter().flatten().map(|found| found.item.clone()).collect();
    action.variables = operation.variables.iter().flatten().filter_map(|binding| match &binding.value {
        VariableValue::Parameter(source) => Some((binding.name.clone(), source.clone())),
        VariableValue::Constant(_) => None,
    }).collect();
    action.args = operation.parameters.iter().map(|parameter| (parameter.name.clone(), parameter.ty.kind_text(parameter.null))).collect();
    action.refetch = operation.refetch.iter().map(|refetch| match &refetch.model {
        Some(model) if model != owner => format!("{model}.{}", refetch.scope),
        _ => refetch.scope.clone(),
    }).collect();
    action.lands = operation.upserts.iter().map(|upsert| ActionLanding { model: upsert.model.clone(), root: upsert.from.item.clone() }).collect();
    action.result = operation.returns.iter().flatten().filter_map(|attribute| match &attribute.ty {
        TypeRef::Scalar(kind) => Some((attribute.name.clone(), scalar_word(*kind).to_string())),
        _ => None,
    }).collect();
    if let (Some(batch), Some(length)) = (&operation.batch, &operation.length) {
        action.batch = Some(dblayer::schema::UploadBatchDeclaration {
            min: length.minimum,
            max: length.maximum,
            items: batch.items.clone(),
            single: batch.single.clone(),
            input: batch.input.iter().cloned().collect(),
            renumber: batch.renumber.as_ref().map(|renumber| dblayer::schema::UploadBatchRenumber { position: renumber.position.item.clone(), size: renumber.size.item.clone(), position_from: renumber.start }),
        });
    }
    if let Some((OptimisticDecl::Fields(fields), _)) = &operation.optimistic {
        action.optimistic = fields.iter().map(|(name, value, _)| (name.clone(), optimistic_json(value))).collect();
    }
    action.expires_after = operation.expires_after;
    action.processing = operation.processing.as_ref().map(|processing| dblayer::schema::ProcessingDeclaration { field: processing.field.clone(), done: processing.done.clone(), progress: processing.progress.clone() });
    action.removes_receiver = operation.removes_receiver.is_some();
    action.copies_receiver_to = operation.copies_receiver_to.as_ref().map(|copy| ReceiverCopy {
        arg: copy.arg.clone(),
        field: copy.field.clone(),
        set: copy.set.iter().map(|(field, value, _)| (field.clone(), match value {
            decl::CopySetDecl::Literal(literal) => CopyValue::Literal(literal.clone()),
            decl::CopySetDecl::Source(source) => CopyValue::Source(build_source(source)),
        })).collect(),
    });
    action.scope_effects = operation.scope_effects.iter().map(|effect| ScopeEffect {
        kind: if effect.joins { ScopeEffectKind::Join } else { ScopeEffectKind::Leave },
        model: effect.model.clone(),
        scope: effect.scope.clone(),
        rows: effect.rows.clone(),
        with: effect.with.iter().cloned().collect(),
        order_param: effect.order.clone(),
        orders: effect.orders.clone(),
    }).collect();
    if operation.kind == OperationKind::Custom && action.variables.is_empty() {
        action.variables.insert(format!("{}Id", lower(owner)), "id".into());
    }
    action
}

fn build_source(source: &SourceDecl) -> EffectSource {
    match source {
        SourceDecl::Owner(field) => EffectSource::Owner(field.clone()),
        SourceDecl::Row { model, row, field } => EffectSource::Row { model: model.clone(), row: row.clone(), field: field.clone() },
    }
}

fn scalar_word(kind: ScalarKind) -> &'static str {
    match kind {
        ScalarKind::Bool => "bool",
        ScalarKind::Int => "int",
        ScalarKind::Num => "num",
        ScalarKind::Id => "id",
        _ => "string",
    }
}

fn optimistic_json(value: &OptimisticValue) -> Json {
    match value {
        OptimisticValue::Literal(value) => value.clone(),
        OptimisticValue::Parameter(name) => json!({ "$arg": name }),
        OptimisticValue::Mapped { from, map } => {
            let map: Map<String, Json> = map.iter().cloned().collect();
            json!({ "$map": { "from": from, "map": map } })
        }
        OptimisticValue::Include { source, when } => match build_source(source) {
            EffectSource::Row { model, row, field } => json!({ "$flag": { "when": when, "include": { "of": model, "row": row, "field": field } } }),
            _ => Json::Null,
        },
        OptimisticValue::AtLeast { min, when } => json!({ "$flag": { "when": when, "at_least": min } }),
        OptimisticValue::Keyed { by, element } => {
            let element: Map<String, Json> = element.iter().map(|(key, part)| (key.clone(), match part {
                OptimisticElement::Literal(value) => value.clone(),
                OptimisticElement::Parameter(name) => json!({ "$arg": name }),
            })).collect();
            json!({ "$keyed": { "by": by, "element": element } })
        }
    }
}

#[cfg(test)]
mod tests;
