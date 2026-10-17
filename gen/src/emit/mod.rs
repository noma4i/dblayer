use dblayer::schema::{FieldKind, FieldSpec, ModelSpec, RemoteShape, RelationSpec, Sort};
use dblayer::value::ScalarKind;
use std::fmt::Write;

mod nested;
mod wire;
mod inputs;
mod enums;
mod operations;
mod unions;
use unions::{emit_kotlin_union, emit_swift_union};
use nested::{emit_kotlin_type, emit_swift_type};
use inputs::{emit_kotlin_inputs, emit_swift_inputs};
use enums::{emit_kotlin_enums, emit_swift_enums};
use operations::{emit_kotlin_operations, emit_swift_operations};
use crate::operation::OperationCall;
use crate::schema_check::SchemaTypes;
use crate::enums::{merge_enums, EnumDecl};

/// The relation that holds the one row of a model: the only global relation answered as a single record.
fn singleton_relation(model: &ModelSpec) -> Option<&RelationSpec> {
    let mut found = model.relations.iter().filter(|relation| relation.is_global && relation.remote.as_ref().is_some_and(|remote| remote.shape == RemoteShape::Single));
    let relation = found.next()?;
    found.next().is_none().then_some(relation)
}

fn name(text: &str) -> String {
    let upper = crate::build::camel(text);
    let mut chars = upper.chars();
    chars.next().map(|first| first.to_lowercase().collect::<String>() + chars.as_str()).unwrap_or_default()
}

fn result_type(class: &str, action: &str) -> String {
    let method = name(action);
    let mut chars = method.chars();
    format!("{class}{}{}Result", chars.next().map(|first| first.to_uppercase().collect::<String>()).unwrap_or_default(), chars.as_str())
}

fn scalar(kind: ScalarKind, swift: bool) -> &'static str {
    match (kind, swift) {
        (ScalarKind::Bool, true) => "Bool",
        (ScalarKind::Bool, false) => "Boolean",
        (ScalarKind::Int, true) => "Int",
        (ScalarKind::Int, false) => "Long",
        (ScalarKind::Num, _) => "Double",
        _ => "String",
    }
}

fn raw_type(swift: bool) -> String {
    if swift { "JSONValue".into() } else { "JsonElement".into() }
}

fn column_type(field: &FieldSpec, swift: bool) -> String {
    match (&field.kind, &field.app_type) {
        (FieldKind::Union, Some(name)) => format!("{name}Kind"),
        _ => field_type(field, swift),
    }
}

fn field_type(field: &FieldSpec, swift: bool) -> String {
    let base = match &field.kind {
        FieldKind::Scalar { kind } => scalar(*kind, swift).to_string(),
        FieldKind::Enum { .. } => field.app_type.clone().expect("an enum field names its type"),
        FieldKind::List { element } => if swift { format!("[{}]", scalar(*element, true)) } else { format!("List<{}>", scalar(*element, false)) },
        FieldKind::Array => field.app_type.as_ref().map(|name| if swift { format!("[{name}]") } else { format!("List<{name}>") }).unwrap_or_else(|| raw_type(swift)),
        FieldKind::Object | FieldKind::Union => field.app_type.clone().unwrap_or_else(|| raw_type(swift)),
        _ => raw_type(swift),
    };
    if field.nullable || field.optional { format!("{base}?") } else { base }
}

pub(crate) fn enum_decls(models: &[ModelSpec], operations: &[OperationCall], schema_types: &SchemaTypes) -> Vec<EnumDecl> {
    fn walk(field: &FieldSpec, out: &mut Vec<EnumDecl>) {
        if let (FieldKind::Enum { values }, Some(name)) = (&field.kind, &field.app_type) { out.push(EnumDecl { name: name.clone(), values: values.clone() }); }
        field.fields.iter().for_each(|inner| walk(inner, out));
    }
    let mut found = schema_types.enums.clone();
    for model in models { model.fields.iter().chain(model.ephemeral.iter().map(|field| &field.field)).for_each(|field| walk(field, &mut found)); }
    for operation in operations { if let Some(root) = &operation.result { walk(root, &mut found); } }
    merge_enums(found).unwrap_or_default()
}

pub(crate) fn declared_types(models: &[ModelSpec]) -> Vec<&FieldSpec> {
    fn walk<'a>(fields: &'a [FieldSpec], out: &mut Vec<&'a FieldSpec>) {
        for field in fields.iter().filter(|field| !field.fields.is_empty()) {
            out.push(field);
            walk(&field.fields, out);
        }
    }
    let mut out = Vec::new();
    for model in models {
        walk(&model.fields, &mut out);
    }
    out
}

fn is_nullable(field: &FieldSpec) -> bool {
    field.nullable || field.optional
}

fn is_id(field: &FieldSpec) -> bool {
    matches!(field.kind, FieldKind::Scalar { kind: ScalarKind::Id })
}

fn key_of(field: &FieldSpec) -> &str {
    field.from.as_deref().unwrap_or(&field.name)
}

fn local_name(model: &ModelSpec, stem: &str) -> String {
    let used: std::collections::HashSet<_> = model.fields.iter().map(|field| field.name.as_str())
        .chain(model.ephemeral.iter().map(|field| field.field.name.as_str()))
        .chain(model.actions.iter().flat_map(|action| action.args.keys().map(String::as_str)))
        .chain(model.relations.iter().flat_map(|relation| relation.by_columns.iter().chain(&relation.text_params).map(String::as_str)))
        .collect();
    let mut name = stem.to_string();
    while used.contains(name.as_str()) { name.push('_'); }
    name
}

fn create_fields<'a>(model: &ModelSpec, fields: &[&'a FieldSpec]) -> Vec<&'a FieldSpec> {
    let sources: Option<Vec<&str>> = model.actions.iter().find(|action| action.name == "create").filter(|action| !action.variables.is_empty()).map(|action| action.variables.values().chain(action.local.iter()).map(String::as_str).collect());
    fields.iter().copied().filter(|field| sources.as_ref().is_none_or(|names| names.contains(&field.name.as_str()))).collect()
}

fn standard_args<'a>(model: &'a ModelSpec, action: &str) -> Vec<(&'a str, &'a str)> {
    model.actions.iter().find(|declared| declared.name == action).map(|declared| declared.args.iter().map(|(name, kind)| (name.as_str(), kind.as_str())).collect()).unwrap_or_default()
}

fn arg_type(kind: &str, swift: bool) -> String {
    let kind = kind.trim_end_matches('?');
    let name = match (kind, swift) {
        ("bool", true) => "Bool",
        ("bool", false) => "Boolean",
        ("int", _) | ("long", true) => "Int",
        ("long", false) => "Long",
        ("num", _) => "Double",
        ("ids", true) => "[String]",
        ("ids", false) => "List<String>",
        _ if crate::operation::is_input_object(kind) => kind,
        _ => "String",
    };
    name.to_string()
}

fn swift_default(field: &FieldSpec) -> Option<String> {
    if field.nullable || field.optional { return Some("nil".into()); }
    match (&field.kind, field.default.as_ref()?) {
        (FieldKind::Scalar { kind: ScalarKind::Bool }, serde_json::Value::Bool(flag)) => Some(flag.to_string()),
        (FieldKind::Scalar { kind: ScalarKind::Int | ScalarKind::Num }, serde_json::Value::Number(number)) => Some(number.to_string()),
        (FieldKind::Scalar { kind: ScalarKind::Str | ScalarKind::Date | ScalarKind::Id }, serde_json::Value::String(text)) => Some(format!("{text:?}")),
        (FieldKind::Enum { .. }, value @ serde_json::Value::String(_)) => Some(nested::literal(field, value, true, &[])),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    Public,
    Internal,
}

/// Swift source for the models. `Internal` drops `public` for apps that generate the models into their own module.
pub fn emit_swift_with(models: &[ModelSpec], operations: &[OperationCall], schema_types: &SchemaTypes, access: Access) -> String {
    let source = emit_swift(models, operations, schema_types);
    if access == Access::Public {
        return source;
    }
    source.lines().map(|line| {
        let indent = line.len() - line.trim_start().len();
        match line.trim_start().strip_prefix("public ") {
            Some(rest) => format!("{}{}", &line[..indent], rest),
            None => line.to_string(),
        }
    }).collect::<Vec<_>>().join("\n") + "\n"
}

pub fn emit_swift(models: &[ModelSpec], operations: &[OperationCall], schema_types: &SchemaTypes) -> String {
    let mut out = "import Foundation\nimport DBlayer\n\n".to_string();
    for model in models {
        let class = model.key.as_str();
        let values = local_name(model, "values");
        let built = local_name(model, "built");
        writeln!(out, "public struct {class}: {} {{\n    public static let modelName = \"{class}\"", if singleton_relation(model).is_some() { "DBSingleton" } else { "DBModel" }).unwrap();
        for field in model.fields.iter().chain(model.ephemeral.iter().map(|field| &field.field)) {
            writeln!(out, "    public let {}: {}", field.name, field_type(field, true)).unwrap();
        }
        writeln!(out, "    public let sync: SyncInfo\n    public let upload: UploadInfo?\n").unwrap();
        let fields: Vec<&FieldSpec> = model.fields.iter().chain(model.ephemeral.iter().map(|field| &field.field)).collect();
        let params: Vec<String> = fields.iter().map(|field| {
            let default = swift_default(field).map(|value| format!(" = {value}")).unwrap_or_default();
            format!("{}: {}{}", field.name, field_type(field, true), default)
        }).chain(["sync: SyncInfo = SyncInfo()".to_string(), "upload: UploadInfo? = nil".to_string()]).collect();
        writeln!(out, "    public init({}) {{", params.join(", ")).unwrap();
        for field in fields.iter().map(|field| field.name.as_str()).chain(["sync", "upload"]) { writeln!(out, "        self.{field} = {field}").unwrap(); }
        out.push_str("    }\n\n");
        writeln!(out, "    public static let wireLayout = \"{}\"\n\n    public init(wire row: WireRow) throws {{\n        let r = CellReader(row.cells)\n        self.id = row.id", wire::layout_of(models, model)).unwrap();
        for (at, field) in wire::wired(model).into_iter().enumerate() { writeln!(out, "        self.{} = {}", field.name, wire::read_swift(field, at, &declared_types(models))).unwrap(); }
        out.push_str("        self.sync = SyncInfo(wire: row.sync)\n        self.upload = row.upload.map(UploadInfo.init(wire:))\n    }\n\n");
        for field in &model.fields {
            if field.name != "id" { writeln!(out, "    public static let {} = Column<{class}, {}>(\"{}\")", field.name, column_type(field, true), field.name).unwrap(); }
        }
        out.push_str("    public static func find(_ id: String) -> LiveSource<Self?> { Query<Self>().find(id) }\n    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }\n");
        for relation in &model.relations {
            let method = name(&relation.name);
            if relation.by_columns.is_empty() && relation.text_params.is_empty() { writeln!(out, "    public static var {method}: Query<Self> {{").unwrap(); }
            else {
                let params = relation.by_columns.iter().map(|key| format!("_ {key}: {}", model.field(key).map(|field| field_type(field, true)).unwrap_or("String".into()))).chain(relation.text_params.iter().map(|key| format!("_ {key}: String"))).collect::<Vec<_>>().join(", ");
                writeln!(out, "    public static func {method}({params}) -> Query<Self> {{").unwrap();
            }
            let params = relation.by_columns.iter().chain(relation.text_params.iter()).map(|key| format!("\"{key}\": JSONValue.encode({key})")).collect::<Vec<_>>().join(", ");
            writeln!(out, "        {} {built} = Query<Self>(relation: \"{}\", params: [{}])", if relation.remote.is_none() { "var" } else { "let" }, relation.name, if params.is_empty() { ":" } else { &params }).unwrap();
            if relation.remote.is_none() {
                for key in &relation.by_columns { writeln!(out, "        {built} = {built}.where(Condition.equal(\"{key}\", JSONValue.encode({key})))").unwrap(); }
                if let Sort::Fields(fields) = &relation.sort {
                    for field in fields { writeln!(out, "        {built} = {built}.order(Order(field: \"{}\", direction: \"{}\"))", field.column, field.direction.name()).unwrap(); }
                }
            }
            writeln!(out, "        return {built}\n    }}").unwrap();
        }
        if let Some(relation) = singleton_relation(model) { writeln!(out, "    public static var singleton: Query<Self> {{ {} }}", name(&relation.name)).unwrap(); }
        let fields: Vec<_> = model.fields.iter().filter(|field| field.name != "id" && !field.is_local()).collect();
        let create_fields = create_fields(model, &fields);
        let args = local_name(model, "args");
        let create_args = standard_args(model, "create");
        let params = create_fields.iter().map(|field| {
            let optional = field.nullable || field.optional || field.default.is_some();
            let mut kind = field_type(field, true);
            if optional && !kind.ends_with('?') { kind.push('?'); }
            format!("{}: {}{}", field.name, kind, if optional { " = nil" } else { "" })
        }).chain(create_args.iter().map(|(name, kind)| format!("{name}: {}{}{}", arg_type(kind, true), if kind.ends_with('?') { "?" } else { "" }, if kind.ends_with('?') { " = nil" } else { "" }))).collect::<Vec<_>>().join(", ");
        writeln!(out, "    public static func create({params}) async throws -> Self {{\n        var {values}: [String: JSONValue] = [:]").unwrap();
        if !create_args.is_empty() { writeln!(out, "        var {args}: [String: JSONValue] = [:]").unwrap(); }
        for field in &create_fields {
            if field.nullable || field.optional || field.default.is_some() { writeln!(out, "        if let value = {} {{ {values}[\"{}\"] = JSONValue.encode(value) }}", field.name, field.name).unwrap(); }
            else { writeln!(out, "        {values}[\"{}\"] = JSONValue.encode({})", field.name, field.name).unwrap(); }
        }
        for (name, kind) in &create_args {
            if kind.ends_with('?') { writeln!(out, "        if let value = {name} {{ {args}[\"{name}\"] = JSONValue.encode(value) }}").unwrap(); }
            else { writeln!(out, "        {args}[\"{name}\"] = JSONValue.encode({name})").unwrap(); }
        }
        writeln!(out, "        return try await create(values: {values}{})\n    }}", if create_args.is_empty() { String::new() } else { format!(", args: {args}") }).unwrap();
        let update_args = standard_args(model, "update");
        let params = fields.iter().map(|field| format!("{}: {}? = nil", field.name, field_type(field, true))).chain(update_args.iter().map(|(name, kind)| format!("{name}: {}? = nil", arg_type(kind, true)))).collect::<Vec<_>>().join(", ");
        writeln!(out, "    public func update({params}) async throws -> Self {{\n        var {values}: [String: JSONValue] = [:]").unwrap();
        if !update_args.is_empty() { writeln!(out, "        var {args}: [String: JSONValue] = [:]").unwrap(); }
        for field in &fields { writeln!(out, "        if let value = {} {{ {values}[\"{}\"] = JSONValue.encode(value) }}", field.name, field.name).unwrap(); }
        for (name, _) in &update_args { writeln!(out, "        if let value = {name} {{ {args}[\"{name}\"] = JSONValue.encode(value) }}").unwrap(); }
        writeln!(out, "        return try await update(values: {values}{})\n    }}", if update_args.is_empty() { String::new() } else { format!(", args: {args}") }).unwrap();
        if !model.ephemeral.is_empty() {
            let params = model.ephemeral.iter().map(|field| format!("{}: {}? = nil", field.field.name, field_type(&field.field, true))).collect::<Vec<_>>().join(", ");
            writeln!(out, "    public func set({params}) async throws {{\n        var {values}: [String: JSONValue] = [:]").unwrap();
            for field in &model.ephemeral { writeln!(out, "        if let value = {} {{ {values}[\"{}\"] = JSONValue.encode(value) }}", field.field.name, field.field.name).unwrap(); }
            writeln!(out, "        try await set(values: {values})\n    }}").unwrap();
        }
        for association in &model.associations {
            let target = association.target.as_str();
            writeln!(out, "    public var {}: LiveSource<[{}]> {{ related(\"{}\", as: {}.self) }}", association.name, target, association.name, target).unwrap();
        }
        for action in model.actions.iter().filter(|action| !["create", "update", "destroy"].contains(&action.name.as_str())) {
            let params = action.args.iter().map(|(arg, kind)| format!("{arg}: {}{}", arg_type(kind, true), if kind.ends_with('?') { "? = nil" } else { "" })).collect::<Vec<_>>().join(", ");
            let literals: serde_json::Map<String, serde_json::Value> = action.optimistic.iter().filter(|(_, value)| value.get("$arg").is_none()).map(|(key, value)| (key.clone(), value.clone())).collect();
            let returns = if action.result.is_empty() { "Self".to_string() } else { format!("ActionAnswer<Self, {}>", result_type(class, &action.name)) };
            writeln!(out, "    public func {}({params}) async throws -> {returns} {{", name(&action.name)).unwrap();
            writeln!(out, "        {} {values} = JSONValue.parse(#\"{}\"#).objectValue", if action.optimistic.values().any(|value| value.get("$arg").is_some()) { "var" } else { "let" }, serde_json::to_string(&literals).unwrap()).unwrap();
            for (field, value) in &action.optimistic { if let Some(arg) = value.get("$arg").and_then(|arg| arg.as_str()) { writeln!(out, "        {values}[\"{field}\"] = JSONValue.encode({arg})").unwrap(); } }
            let args = action.args.keys().map(|arg| format!("\"{arg}\": JSONValue.encode({arg})")).collect::<Vec<_>>().join(", ");
            writeln!(out, "        return try await {}(\"{}\", values: {values}, args: [{}])\n    }}", if action.result.is_empty() { "act" } else { "actAnswering" }, action.name, if args.is_empty() { ":" } else { &args }).unwrap();
        }
        out.push_str("}\n\n");
        for action in model.actions.iter().filter(|action| !action.result.is_empty()) {
            writeln!(out, "public struct {}: Codable, Sendable {{", result_type(class, &action.name)).unwrap();
            for (field, kind) in &action.result { writeln!(out, "    public let {field}: {}?", arg_type(kind, true)).unwrap(); }
            out.push_str("}\n\n");
        }
    }
    let types = declared_types(models);
    for declaration in &types { if declaration.kind == FieldKind::Union { emit_swift_union(&mut out, declaration); } else { emit_swift_type(&mut out, declaration, &types, true); } }
    emit_swift_enums(&mut out, &enum_decls(models, operations, schema_types));
    emit_swift_inputs(&mut out, &schema_types.inputs);
    emit_swift_operations(&mut out, operations);
    if !models.is_empty() { writeln!(out, "public let modelsJSON = #\"{}\"#", serde_json::to_string(models).unwrap()).unwrap(); }
    out
}

pub fn emit_kotlin(models: &[ModelSpec], operations: &[OperationCall], schema_types: &SchemaTypes) -> String {
    let encoding = if schema_types.inputs.is_empty() { "" } else { "import kotlinx.serialization.EncodeDefault\nimport kotlinx.serialization.ExperimentalSerializationApi\n" };
    let mut out = format!("package com.noma4i.dblayer.models\n\nimport com.noma4i.dblayer.*\n{encoding}import kotlinx.serialization.SerialName\nimport kotlinx.serialization.Serializable\nimport kotlinx.serialization.json.*\n\n");
    for model in models {
        let class = model.key.as_str();
        let values = local_name(model, "values");
        let built = local_name(model, "built");
        writeln!(out, "data class {class}(").unwrap();
        for field in model.fields.iter().chain(model.ephemeral.iter().map(|field| &field.field)) {
            writeln!(out, "    {}val {}: {}{},", if field.name == "id" { "override " } else { "" }, field.name, field_type(field, false), if field.nullable || field.optional { " = null" } else { "" }).unwrap();
        }
        writeln!(out, "    override val sync: SyncInfo = SyncInfo(),\n    override val upload: UploadInfo? = null\n) : DBModel {{\n    override val modelName get() = \"{class}\"\n    companion object : ModelDecoder<{class}> {{\n        override val layout = \"{}\"\n        override val modelName = \"{class}\"\n        override fun decode(row: WireRow): {class} {{\n            val r = CellReader(row.cells)\n            return {class}(\n                id = row.id,\n{}                sync = SyncInfo.of(row.sync),\n                upload = row.upload?.let {{ UploadInfo.of(it) }},\n            )\n        }}\n", wire::layout_of(models, model), wire::wired(model).into_iter().enumerate().map(|(at, field)| format!("                {} = {},\n", field.name, wire::read_kotlin(field, at, &declared_types(models)))).collect::<String>()).unwrap();
        for field in model.fields.iter().filter(|field| field.name != "id") { writeln!(out, "        val {} = Column<{class}, {}>(\"{}\")", field.name, column_type(field, false), field.name).unwrap(); }
        writeln!(out, "        fun find(id: String) = Query(\"{class}\", this).find(id)\n        fun where(condition: Condition) = Query(\"{class}\", this).where(condition)\n        fun ids(ids: List<String>) = idsQuery(\"{class}\", this, ids)\n        fun entity(id: String) = entityOf(\"{class}\", this, id)").unwrap();
        for relation in &model.relations {
            let method = name(&relation.name);
            let params = relation.by_columns.iter().map(|key| format!("{key}: {}", model.field(key).map(|field| field_type(field, false)).unwrap_or("String".into()))).chain(relation.text_params.iter().map(|key| format!("{key}: String"))).collect::<Vec<_>>().join(", ");
            if params.is_empty() { writeln!(out, "        val {method}: Query<{class}> get() {{").unwrap(); }
            else { writeln!(out, "        fun {method}({params}): Query<{class}> {{").unwrap(); }
            let params = relation.by_columns.iter().map(|key| format!("\"{key}\" to jsonValue({key})")).collect::<Vec<_>>().join(", ");
            writeln!(out, "            var {built} = Query(\"{class}\", this, relation = \"{}\", params = buildJsonObject {{ {} }})", relation.name, relation.by_columns.iter().chain(relation.text_params.iter()).map(|key| format!("put(\"{key}\", jsonValue({key}))")).collect::<Vec<_>>().join("; ")).unwrap();
            let _ = params;
            if relation.remote.is_none() {
                for key in &relation.by_columns { writeln!(out, "            {built} = {built}.where(Condition.equal(\"{key}\", jsonValue({key})))").unwrap(); }
                if let Sort::Fields(fields) = &relation.sort { for field in fields { writeln!(out, "            {built} = {built}.order(Order(\"{}\", \"{}\"))", field.column, field.direction.name()).unwrap(); } }
            }
            writeln!(out, "            return {built}\n        }}").unwrap();
        }
        if let Some(relation) = singleton_relation(model) { writeln!(out, "        fun entity() = singletonEntityOf(\"{class}\", this, {})", name(&relation.name)).unwrap(); }
        let fields: Vec<_> = model.fields.iter().filter(|field| field.name != "id" && !field.is_local()).collect();
        let create_fields = create_fields(model, &fields);
        let args = local_name(model, "args");
        let create_args = standard_args(model, "create");
        let params = create_fields.iter().map(|field| {
            let optional = field.nullable || field.optional || field.default.is_some();
            let mut kind = field_type(field, false);
            if optional && !kind.ends_with('?') { kind.push('?'); }
            format!("{}: {}{}", field.name, kind, if optional { " = null" } else { "" })
        }).chain(create_args.iter().map(|(name, kind)| format!("{name}: {}{}", arg_type(kind, false), if kind.ends_with('?') { "? = null" } else { "" }))).collect::<Vec<_>>().join(", ");
        writeln!(out, "        suspend fun create({params}): {class} {{\n            val {values} = buildJsonObject {{").unwrap();
        for field in &create_fields {
            if field.nullable || field.optional || field.default.is_some() { writeln!(out, "                {}?.let {{ put(\"{}\", jsonValue(it)) }}", field.name, field.name).unwrap(); }
            else { writeln!(out, "                put(\"{}\", jsonValue({}))", field.name, field.name).unwrap(); }
        }
        out.push_str("            }\n");
        if !create_args.is_empty() {
            writeln!(out, "            val {args} = buildJsonObject {{").unwrap();
            for (name, kind) in &create_args {
                if kind.ends_with('?') { writeln!(out, "                {name}?.let {{ put(\"{name}\", jsonValue(it)) }}").unwrap(); }
                else { writeln!(out, "                put(\"{name}\", jsonValue({name}))").unwrap(); }
            }
            out.push_str("            }\n");
        }
        writeln!(out, "            return read(DBlayer.database.create(\"{class}\", {values}.toString(), {}))\n        }}\n    }}", if create_args.is_empty() { "\"{}\"".to_string() } else { format!("{args}.toString()") }).unwrap();
        let update_args = standard_args(model, "update");
        let params = fields.iter().map(|field| format!("{}: {}{} = null", field.name, field_type(field, false), if field.nullable || field.optional { "" } else { "?" })).chain(update_args.iter().map(|(name, kind)| format!("{name}: {}? = null", arg_type(kind, false)))).collect::<Vec<_>>().join(", ");
        writeln!(out, "    suspend fun update({params}): {class} {{\n        val {values} = buildJsonObject {{").unwrap();
        for field in &fields { writeln!(out, "            {}?.let {{ put(\"{}\", jsonValue(it)) }}", field.name, field.name).unwrap(); }
        out.push_str("        }\n");
        if !update_args.is_empty() {
            writeln!(out, "        val {args} = buildJsonObject {{").unwrap();
            for (name, _) in &update_args { writeln!(out, "            {name}?.let {{ put(\"{name}\", jsonValue(it)) }}").unwrap(); }
            out.push_str("        }\n");
        }
        writeln!(out, "        return {class}.read(DBlayer.database.update(modelName, id, {values}.toString(), {}))\n    }}", if update_args.is_empty() { "\"{}\"".to_string() } else { format!("{args}.toString()") }).unwrap();
        if !model.ephemeral.is_empty() {
            let params = model.ephemeral.iter().map(|field| format!("{}: {}{} = null", field.field.name, field_type(&field.field, false), if field.field.nullable || field.field.optional { "" } else { "?" })).collect::<Vec<_>>().join(", ");
            writeln!(out, "    suspend fun set({params}) {{\n        DBlayer.database.setEphemeral(modelName, id, buildJsonObject {{").unwrap();
            for field in &model.ephemeral { writeln!(out, "            {}?.let {{ put(\"{}\", jsonValue(it)) }}", field.field.name, field.field.name).unwrap(); }
            out.push_str("        }.toString())\n    }\n");
        }
        for action in model.actions.iter().filter(|action| !["create", "update", "destroy"].contains(&action.name.as_str())) {
            let params = action.args.iter().map(|(arg, kind)| format!("{arg}: {}{}", arg_type(kind, false), if kind.ends_with('?') { "? = null" } else { "" })).collect::<Vec<_>>().join(", ");
            let literals: serde_json::Map<String, serde_json::Value> = action.optimistic.iter().filter(|(_, value)| value.get("$arg").is_none()).map(|(key, value)| (key.clone(), value.clone())).collect();
            let from_args = action.optimistic.iter().filter_map(|(field, value)| value.get("$arg").and_then(|arg| arg.as_str()).map(|arg| format!("\"{field}\" to jsonValue({arg})"))).collect::<Vec<_>>().join(", ");
            let args = action.args.keys().map(|arg| format!("\"{arg}\" to jsonValue({arg})")).collect::<Vec<_>>().join(", ");
            let call = format!("\"{}\", JsonObject(parseJsonObject(\"\"\"{}\"\"\") + mapOf({from_args})), JsonObject(mapOf({args})), {class}", action.name, serde_json::to_string(&literals).unwrap().replace('$', "${'$'}"));
            if action.result.is_empty() {
                writeln!(out, "    suspend fun {}({params}): {class} = act({call})", name(&action.name)).unwrap();
            } else {
                let result = result_type(class, &action.name);
                writeln!(out, "    suspend fun {}({params}): ActionAnswer<{class}, {result}> = actAnswering({call}, {result}.serializer())", name(&action.name)).unwrap();
            }
        }
        for association in &model.associations { writeln!(out, "    val {} get() = related(\"{}\", {})", association.name, association.name, association.target).unwrap(); }
        out.push_str("}\n\n");
        for action in model.actions.iter().filter(|action| !action.result.is_empty()) {
            let fields = action.result.iter().map(|(field, kind)| format!("val {field}: {}? = null", arg_type(kind, false))).collect::<Vec<_>>().join(", ");
            writeln!(out, "@Serializable\ndata class {}({fields})\n", result_type(class, &action.name)).unwrap();
        }
    }
    let types = declared_types(models);
    for declaration in &types { if declaration.kind == FieldKind::Union { emit_kotlin_union(&mut out, declaration); } else { emit_kotlin_type(&mut out, declaration, &types, true); } }
    emit_kotlin_enums(&mut out, &enum_decls(models, operations, schema_types));
    emit_kotlin_inputs(&mut out, &schema_types.inputs);
    emit_kotlin_operations(&mut out, operations);
    let models_json = serde_json::to_string(models).unwrap().replace('$', "${'$'}");
    if !models.is_empty() { writeln!(out, "val modelsJSON = \"\"\"{models_json}\"\"\"").unwrap(); }
    out
}
