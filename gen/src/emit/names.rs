use dblayer::schema::{FieldSpec, ModelSpec, RelationSpec};

use super::inputs::case_name;
use super::{create_fields, declared_types, enum_decls, standard_args, writable_fields, ACCOUNT, ROW_ID};
use crate::operation::OperationCall;
use crate::schema_check::SchemaTypes;

/// Kotlin `read`, `put`, `act` and `actAnswering` are not listed: the templates call them as `Model.read`, `this.put`, `this.act`.
const BARE_CALLS: &[&str] = &["self", "this", "jsonValue", "mapOf", "mutableMapOf", "buildJsonObject", "parseJsonObject", "requireNotNull", "onAccount", "listOf", "emptyMap"];

pub(super) const MODEL_NAME: &str = "modelName";
pub(super) const WIRE_LAYOUT: &str = "wireLayout";
pub(super) const LAYOUT: &str = "layout";
pub(super) const SYNC: &str = "sync";
pub(super) const UPLOADS: &str = "uploads";
pub(super) const SINGLETON: &str = "singleton";

pub const MODEL_MEMBERS: &[&str] = &[MODEL_NAME, WIRE_LAYOUT, LAYOUT, SYNC, UPLOADS, SINGLETON];

const IMPORTED_TYPES: &[&str] = &[
    "Query", "Condition", "Order", "Column", "Association", "FileField", "FileInfo", "JSONValue", "JsonElement", "JsonObject", "JsonArray", "JsonPrimitive", "JsonNull", "JsonEncoder", "DB", "DBlayer", "DBModel", "DBSingleton",
    "ModelDecoder", "AccountScope", "Sent", "ActionAnswer", "UploadRequest", "UploadInfo", "SyncInfo", "WireRow", "Cell", "CellReader", "Blob", "IDText", "IDTexts", "GraphQLNullable", "ListSerializer", "UnionSerializer",
    "UnionTypenameKey", "RawEnumSerializer", "RawEnum", "KSerializer", "SerialDescriptor", "SerializationException", "SerialName", "Serializable", "Immutable", "EncodeDefault", "ExperimentalSerializationApi", "OptIn",
    "Operations", "Self", "Pair", "Date", "String", "Int", "Long", "Bool", "Boolean", "Double", "List", "Map", "Dictionary", "Array", "Codable", "Encodable", "Equatable", "Hashable", "Sendable", "Encoder", "Decoder",
    "CodingKey", "CodingKeys", "DecodingError",
];

const RENAME: &str = "Rename it; the server name stays in `from:` or `variables`";
const RENAME_IN_SCHEMA: &str = "Rename it in the schema";

struct Site {
    subject: String,
    site: String,
    kind: &'static str,
    names: Vec<String>,
    fixed: Vec<&'static str>,
    rename: &'static str,
}

fn is_hidden_action(name: &str) -> bool {
    ["create", "update", "destroy"].contains(&name)
}

fn edge_accessor(relation: &RelationSpec) -> bool {
    !relation.edge_fields.is_empty() && relation.node.is_none()
}

fn field_names(fields: &[FieldSpec]) -> Vec<String> {
    fields.iter().map(|field| field.name.clone()).collect()
}

fn typed_fields<'a>(root: &'a FieldSpec, out: &mut Vec<&'a FieldSpec>) {
    if !root.fields.is_empty() { out.push(root); }
    root.fields.iter().for_each(|inner| typed_fields(inner, out));
}

fn declared_in_results<'a>(operations: &'a [OperationCall]) -> Vec<&'a FieldSpec> {
    let mut out = Vec::new();
    operations.iter().filter_map(|operation| operation.result.as_ref()).for_each(|root| typed_fields(root, &mut out));
    out
}

fn sites(models: &[ModelSpec], operations: &[OperationCall], inputs: &SchemaTypes) -> Vec<Site> {
    let mut found = Vec::new();
    for model in models {
        let subject = model.key.as_str().to_string();
        let owner = format!("model `{subject}`");
        let mut push = |site: String, kind: &'static str, names: Vec<String>, fixed: Vec<&'static str>| found.push(Site { subject: subject.clone(), site, kind, names, fixed, rename: RENAME });
        let fields = writable_fields(model);
        let create: Vec<String> = create_fields(model, &fields).iter().map(|field| field.name.clone()).chain(standard_args(model, "create").iter().map(|(name, _)| name.to_string())).collect();
        push(format!("{owner} create"), "parameter", create, vec![ACCOUNT]);
        let update: Vec<String> = fields.iter().map(|field| field.name.clone()).chain(standard_args(model, "update").iter().map(|(name, _)| name.to_string())).collect();
        push(format!("{owner} update"), "parameter", update, vec![ACCOUNT]);
        if !model.ephemeral.is_empty() {
            push(format!("{owner} set"), "parameter", model.ephemeral.iter().map(|field| field.field.name.clone()).collect(), vec![ACCOUNT]);
        }
        let constructor = model.fields.iter().chain(model.ephemeral.iter().map(|field| &field.field)).map(|field| field.name.clone()).collect();
        push(format!("{owner} constructor"), "field", constructor, MODEL_MEMBERS.to_vec());
        for action in model.actions.iter().filter(|action| !action.upload && !is_hidden_action(&action.name)) {
            push(format!("{owner} action `{}`", action.name), "parameter", action.args.keys().cloned().collect(), vec![ACCOUNT]);
            push(format!("{owner} action `{}` result", action.name), "field", action.result.keys().cloned().collect(), Vec::new());
        }
        for relation in model.relations.iter().filter(|relation| edge_accessor(relation)) {
            let parameters = relation.by_columns.iter().chain(&relation.text_params).cloned().collect();
            push(format!("{owner} scope `{}` edge accessor", relation.name), "parameter", parameters, vec![ROW_ID, ACCOUNT]);
            push(format!("{owner} scope `{}` edge", relation.name), "field", field_names(&relation.edge_fields), Vec::new());
        }
    }
    for declaration in declared_types(models).into_iter().chain(declared_in_results(operations)) {
        found.push(Site { subject: declaration.app_type.clone().unwrap_or_default(), site: format!("type `{}`", declaration.app_type.as_deref().unwrap_or_default()), kind: "field", names: field_names(&declaration.fields), fixed: Vec::new(), rename: RENAME });
    }
    for operation in operations {
        found.push(Site { subject: operation.name.clone(), site: format!("operation `{}` call", operation.name), kind: "parameter", names: operation.args.iter().map(|(name, _)| name.clone()).collect(), fixed: vec![ACCOUNT], rename: RENAME });
    }
    for input in &inputs.inputs {
        found.push(Site { subject: input.name.clone(), site: format!("input `{}`", input.name), kind: "field", names: input.fields.iter().map(|field| field.name.clone()).collect(), fixed: Vec::new(), rename: RENAME_IN_SCHEMA });
    }
    found
}

fn refusal(name: &str, fixed: &[&str]) -> Option<String> {
    if let Some(fixed) = fixed.iter().find(|fixed| **fixed == name) { return Some(format!("has the name of the generated parameter `{fixed}`")); }
    if BARE_CALLS.contains(&name) { return Some("has the name of a call or receiver that generated code writes bare".to_string()); }
    IMPORTED_TYPES.contains(&name).then(|| "has the name of a type that generated code imports".to_string())
}

fn type_names(models: &[ModelSpec], operations: &[OperationCall], inputs: &SchemaTypes) -> Vec<(String, String)> {
    let declared = declared_types(models).into_iter().chain(declared_in_results(operations)).filter_map(|declaration| declaration.app_type.clone());
    let enums = enum_decls(models, operations, inputs).unwrap_or_default().into_iter().map(|declaration| declaration.name);
    models.iter().map(|model| model.key.as_str().to_string()).chain(declared).chain(enums).chain(inputs.inputs.iter().map(|input| input.name.clone())).map(|name| (name.clone(), name)).collect()
}

pub(crate) fn name_clashes(models: &[ModelSpec], operations: &[OperationCall], inputs: &SchemaTypes) -> Vec<(String, String)> {
    let mut found = Vec::new();
    for site in sites(models, operations, inputs) {
        for name in &site.names {
            let Some(why) = refusal(name, &site.fixed) else { continue };
            found.push((site.subject.clone(), format!("{}: {} `{name}` {why}. {}", site.site, site.kind, site.rename)));
        }
    }
    for (subject, name) in type_names(models, operations, inputs) {
        if IMPORTED_TYPES.contains(&name.as_str()) {
            found.push((subject, format!("type `{name}` has the name of a type that generated code imports. Rename it")));
        }
    }
    for input in inputs.inputs.iter().filter(|input| input.one_of) {
        for (at, field) in input.fields.iter().enumerate() {
            if let Some(other) = input.fields[..at].iter().find(|other| case_name(other) == case_name(field)) {
                found.push((input.name.clone(), format!("input `{}`: fields `{}` and `{}` print the same case `{}`. Rename one in the schema", input.name, other.name, field.name, case_name(field))));
            }
        }
    }
    found
}
