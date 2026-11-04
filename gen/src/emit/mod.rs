use dblayer::schema::{FieldKind, FieldSpec, ModelSpec, RemoteShape, RelationSpec, Sort};
use dblayer::value::ScalarKind;
use std::fmt::Write;

mod nested;
mod wire;
mod inputs;
mod enums;
mod operations;
mod unions;
mod nodes;
mod edges;
mod names;
pub(crate) use names::name_clashes;
use dblayer::schema::{LAYOUT, MODEL_NAME, SINGLETON, SYNC, UPLOADS, WIRE_LAYOUT};
use edges::{kotlin_accessors, swift_accessors};
use nodes::{kotlin_node, swift_node};
use unions::{emit_kotlin_union, emit_swift_union};
use nested::{emit_kotlin_type, emit_swift_type};
use inputs::{emit_kotlin_inputs, emit_swift_inputs};
use enums::{emit_kotlin_enums, emit_swift_enums};
use operations::{emit_kotlin_operations, emit_swift_operations};
use crate::operation::OperationCall;
use crate::schema_check::SchemaTypes;
use crate::enums::{declared_in, declared_in_field, identifier, merge_enums, EnumDecl};

pub(crate) const ACCOUNT: &str = "account";
pub(crate) const ROW_ID: &str = "id";

/// The relation that holds the one row of a model: the only global relation answered as a single record.
fn singleton_relation(model: &ModelSpec) -> Option<&RelationSpec> {
    let mut found = model.relations.iter().filter(|relation| relation.is_global && relation.remote.as_ref().is_some_and(|remote| remote.shape == RemoteShape::Single));
    let relation = found.next()?;
    found.next().is_none().then_some(relation)
}

pub(crate) fn name(text: &str) -> String {
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

fn is_file(field: &FieldSpec) -> bool { matches!(field.kind, FieldKind::File) }

fn field_type(field: &FieldSpec, swift: bool) -> String {
    let base = match &field.kind {
        FieldKind::File => "FileInfo".to_string(),
        FieldKind::Scalar { kind } => scalar(*kind, swift).to_string(),
        FieldKind::Enum { .. } => field.app_type.clone().expect("an enum field names its type"),
        FieldKind::List { element } => {
            let item = field.app_type.as_deref().filter(|_| !field.values.is_empty()).unwrap_or_else(|| scalar(*element, swift));
            if swift { format!("[{item}]") } else { format!("List<{item}>") }
        }
        FieldKind::Array | FieldKind::UnionArray => field.app_type.as_ref().map(|name| if swift { format!("[{name}]") } else { format!("List<{name}>") }).unwrap_or_else(|| raw_type(swift)),
        FieldKind::Object | FieldKind::Union => field.app_type.clone().unwrap_or_else(|| raw_type(swift)),
        _ => raw_type(swift),
    };
    if field.nullable || field.optional { format!("{base}?") } else { base }
}

pub(super) fn text_param(relation: &RelationSpec, key: &str, decls: &[EnumDecl], swift: bool) -> String {
    let printed = identifier(key, swift);
    if let Some(kind) = relation.scalar_types.get(key) { return format!("{printed}: {}", scalar(*kind, swift)); }
    let Some(kind) = relation.text_types.get(key) else { return if swift { format!("_ {printed}: String") } else { format!("{printed}: String") } };
    let default = relation.text_defaults.get(key).and_then(|value| {
        let declaration = decls.iter().find(|declaration| &declaration.name == kind)?;
        let case = declaration.cases(swift).into_iter().nth(declaration.values.iter().position(|candidate| candidate == value)?)?;
        Some(if swift { format!(" = .{case}") } else { format!(" = {kind}.{case}") })
    });
    format!("{printed}: {kind}{}", default.unwrap_or_default())
}

pub(crate) fn enum_decls(models: &[ModelSpec], operations: &[OperationCall], schema_types: &SchemaTypes) -> Result<Vec<EnumDecl>, String> {
    let mut found = schema_types.enums.clone();
    found.extend(declared_in(models).into_iter().map(|site| site.decl));
    found.extend(operations.iter().filter_map(|operation| operation.result.as_ref()).flat_map(declared_in_field));
    merge_enums(found)
}

fn checked_enum_decls(models: &[ModelSpec], operations: &[OperationCall], schema_types: &SchemaTypes) -> Vec<EnumDecl> {
    enum_decls(models, operations, schema_types).expect("compile_files refuses enums declared with different values")
}

fn spread_shared(field: &mut FieldSpec) {
    let shared = field.shared.clone();
    let variants = matches!(field.kind, FieldKind::Union | FieldKind::UnionArray);
    for child in &mut field.fields {
        if variants {
            child.fields.splice(0..0, shared.iter().cloned());
            child.fields.iter_mut().for_each(spread_shared);
        } else {
            spread_shared(child);
        }
    }
    field.shared.iter_mut().for_each(spread_shared);
}

/// The models with every shared field of a union written into each of its variants, the form the platform types are emitted from.
fn with_shared_in_variants(models: &[ModelSpec]) -> Vec<ModelSpec> {
    let mut spread = models.to_vec();
    spread.iter_mut().flat_map(|model| model.fields.iter_mut().chain(model.ephemeral.iter_mut().map(|field| &mut field.field))).for_each(spread_shared);
    spread
}

pub(crate) fn declared_types(models: &[ModelSpec]) -> Vec<&FieldSpec> {
    fn walk<'a>(fields: &'a [FieldSpec], out: &mut Vec<&'a FieldSpec>) {
        for field in fields.iter().filter(|field| !field.fields.is_empty()) {
            if !out.iter().any(|seen| seen.app_type == field.app_type && seen.kind == field.kind) {
                out.push(field);
            }
            walk(&field.fields, out);
            walk(&field.shared, out);
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

fn writable_fields(model: &ModelSpec) -> Vec<&FieldSpec> {
    model.fields.iter().filter(|field| field.name != "id" && !field.is_local() && !is_file(field)).collect()
}

fn create_fields<'a>(model: &ModelSpec, fields: &[&'a FieldSpec]) -> Vec<&'a FieldSpec> {
    let sources: Option<Vec<&str>> = model.actions.iter().find(|action| action.name == "create").filter(|action| !action.variables.is_empty()).map(|action| action.variables.values().chain(action.local.iter()).map(String::as_str).collect());
    fields.iter().copied().filter(|field| sources.as_ref().is_none_or(|names| names.contains(&field.name.as_str()))).collect()
}

fn create_optional(model: &ModelSpec, field: &FieldSpec) -> bool {
    field.nullable || field.optional || field.default.is_some() || model.actions.iter().any(|action| action.name == "create" && action.local_from.contains_key(&field.name))
}

fn standard_args<'a>(model: &'a ModelSpec, action: &str) -> Vec<(&'a str, &'a str)> {
    model.actions.iter().find(|declared| declared.name == action).map(|declared| declared.args.iter().map(|(name, kind)| (name.as_str(), kind.as_str())).collect()).unwrap_or_default()
}

fn arg_type(kind: &str, swift: bool) -> String {
    let kind = kind.trim_end_matches('?');
    if let Some(inner) = kind.strip_prefix('[').and_then(|rest| rest.strip_suffix(']')) {
        let item = format!("{}{}", arg_type(inner, swift), if inner.ends_with('?') { "?" } else { "" });
        return if swift { format!("[{item}]") } else { format!("List<{item}>") };
    }
    let name = match (kind, swift) {
        ("blob", _) => "Blob",
        ("bool", true) => "Bool",
        ("bool", false) => "Boolean",
        ("int" | "long", true) => "Int",
        ("int" | "long", false) => "Long",
        ("num", _) => "Double",
        ("ids", true) => "[String]",
        ("ids", false) => "List<String>",
        _ if crate::operation::is_input_object(kind) => kind,
        _ => "String",
    };
    name.to_string()
}

fn kotlin_patch_param(field: &FieldSpec) -> String {
    if field.nullable || field.optional { format!("{}: GraphQLNullable<{}> = GraphQLNullable.Omitted", identifier(&field.name, false), field_type(field, false).trim_end_matches('?')) }
    else { format!("{}: {}? = null", identifier(&field.name, false), field_type(field, false)) }
}

fn kotlin_patch_put(field: &FieldSpec) -> String {
    let printed = identifier(&field.name, false);
    if field.nullable || field.optional { format!("            this.put(\"{}\", {printed})", field.name) }
    else { format!("            {printed}?.let {{ this.put(\"{}\", jsonValue(it)) }}", field.name) }
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
        let trimmed = line.trim_start();
        let (attribute, declaration) = trimmed.strip_prefix("@concurrent ").map_or(("", trimmed), |rest| ("@concurrent ", rest));
        match declaration.strip_prefix("public ") {
            Some(rest) => format!("{}{}{}", &line[..indent], attribute, rest),
            None => line.to_string(),
        }
    }).collect::<Vec<_>>().join("\n") + "\n"
}

pub fn emit_swift(models: &[ModelSpec], operations: &[OperationCall], schema_types: &SchemaTypes) -> String {
    let decls = checked_enum_decls(models, operations, schema_types);
    let mut out = "import Foundation\nimport DBlayer\n\n".to_string();
    let mut edge_types = String::new();
    for model in models {
        let class = model.key.as_str();
        let values = "values_".to_string();
        let scope = "scope_".to_string();
        let built = "built_".to_string();
        writeln!(out, "public struct {class}: {} {{\n    public static let {MODEL_NAME} = \"{class}\"", if singleton_relation(model).is_some() { "DBSingleton" } else { "DBModel" }).unwrap();
        for field in model.fields.iter().chain(model.ephemeral.iter().map(|field| &field.field)) {
            writeln!(out, "    public let {}: {}", identifier(&field.name, true), field_type(field, true)).unwrap();
        }
        writeln!(out, "    public let {SYNC}: SyncInfo\n    public let {UPLOADS}: [String: UploadInfo]\n").unwrap();
        let fields: Vec<&FieldSpec> = model.fields.iter().chain(model.ephemeral.iter().map(|field| &field.field)).collect();
        let params: Vec<String> = fields.iter().map(|field| {
            let default = swift_default(field).map(|value| format!(" = {value}")).unwrap_or_default();
            format!("{}: {}{}", identifier(&field.name, true), field_type(field, true), default)
        }).chain([format!("{SYNC}: SyncInfo = SyncInfo()"), format!("{UPLOADS}: [String: UploadInfo] = [:]")]).collect();
        writeln!(out, "    public init({}) {{", params.join(", ")).unwrap();
        for field in fields.iter().map(|field| field.name.as_str()).chain([SYNC, UPLOADS]) { let field = identifier(field, true); writeln!(out, "        self.{field} = {field}").unwrap(); }
        out.push_str("    }\n\n");
        writeln!(out, "    public static let {WIRE_LAYOUT} = \"{}\"\n\n    public init(wire row: WireRow) throws {{\n        let r = CellReader(row.cells)\n        self.id = row.id", wire::layout_of(models, model)).unwrap();
        for (at, field) in wire::wired(model).into_iter().enumerate() { writeln!(out, "        self.{} = {}", identifier(&field.name, true), wire::read_swift(field, at)).unwrap(); }
        out.push_str(&format!("        self.{SYNC} = SyncInfo(wire: row.sync)\n        self.{UPLOADS} = Dictionary(uniqueKeysWithValues: row.uploads.map {{ ($0.action, UploadInfo(wire: $0.upload)) }})\n    }}\n\n"));
        let files: Vec<&FieldSpec> = model.fields.iter().filter(|field| is_file(field)).collect();
        if !files.is_empty() { writeln!(out, "    public enum Files: String, Sendable {{ case {} }}", files.iter().map(|field| identifier(&field.name, true)).collect::<Vec<_>>().join(", ")).unwrap(); }
        for field in &model.fields {
            if field.name != "id" && !is_file(field) { writeln!(out, "    public static let {} = Column<{class}, {}>(\"{}\")", identifier(&field.name, true), column_type(field, true), field.name).unwrap(); }
        }
        out.push_str("    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }\n");
        for relation in &model.relations {
            let method = identifier(&name(&relation.name), true);
            if let Some(kind) = relation.node { swift_node(&mut out, &method, relation, kind); continue; }
            if relation.by_columns.is_empty() && relation.text_params.is_empty() { writeln!(out, "    public static var {method}: Query<Self> {{").unwrap(); }
            else {
                let params = relation.by_columns.iter().map(|key| format!("_ {}: {}", identifier(key, true), model.field(key).map(|field| field_type(field, true)).unwrap_or("String".into()))).chain(relation.text_params.iter().map(|key| text_param(relation, key, &decls, true))).collect::<Vec<_>>().join(", ");
                writeln!(out, "    public static func {method}({params}) throws -> Query<Self> {{").unwrap();
            }
            let params = relation.by_columns.iter().chain(relation.text_params.iter()).map(|key| format!("\"{key}\": try JSONValue.encode({})", identifier(key, true))).collect::<Vec<_>>().join(", ");
            writeln!(out, "        {} {built} = Query<Self>(relation: \"{}\", params: [{}])", if relation.remote.is_none() { "var" } else { "let" }, relation.name, if params.is_empty() { ":" } else { &params }).unwrap();
            if relation.remote.is_none() {
                for key in &relation.by_columns { writeln!(out, "        {built} = {built}.where(Condition.equal(\"{key}\", try JSONValue.encode({})))", identifier(key, true)).unwrap(); }
                if let Sort::Fields(fields) = &relation.sort {
                    for field in fields { writeln!(out, "        {built} = {built}.order(Order(field: \"{}\", direction: \"{}\"))", field.column, field.direction.name()).unwrap(); }
                }
            }
            writeln!(out, "        return {built}\n    }}").unwrap();
            swift_accessors(&mut out, &mut edge_types, model, relation, &decls);
        }
        if let Some(relation) = singleton_relation(model) { writeln!(out, "    public static var {SINGLETON}: Query<Self> {{ Self.{} }}", identifier(&name(&relation.name), true)).unwrap(); }
        for action in model.actions.iter().filter(|action| action.upload && !action.args.is_empty()) {
            let upload_args = standard_args(model, &action.name);
            let method = name(&action.name);
            let params = upload_args.iter().map(|(arg, kind)| format!("{}: {}{}", identifier(arg, true), arg_type(kind, true), if kind.ends_with('?') { "? = nil" } else { "" })).collect::<Vec<_>>().join(", ");
            writeln!(out, "    public static func {method}Variables({params}) throws -> [String: JSONValue] {{\n        var variables_: [String: JSONValue] = [:]").unwrap();
            for (arg, kind) in &upload_args {
                if kind.ends_with('?') { writeln!(out, "        if let value = {printed} {{ variables_[\"{arg}\"] = try JSONValue.encode(value) }}", printed = identifier(arg, true)).unwrap(); }
                else { writeln!(out, "        variables_[\"{arg}\"] = try JSONValue.encode({printed})", printed = identifier(arg, true)).unwrap(); }
            }
            writeln!(out, "        return variables_\n    }}").unwrap();
        }
        let fields = writable_fields(model);
        let create_fields = create_fields(model, &fields);
        let args = "args_".to_string();
        let create_args = standard_args(model, "create");
        let params = create_fields.iter().map(|field| {
            let optional = create_optional(model, field);
            let mut kind = field_type(field, true);
            if optional && !kind.ends_with('?') { kind.push('?'); }
            format!("{}: {}{}", identifier(&field.name, true), kind, if optional { " = nil" } else { "" })
        }).chain(create_args.iter().map(|(name, kind)| format!("{}: {}{}{}", identifier(name, true), arg_type(kind, true), if kind.ends_with('?') { "?" } else { "" }, if kind.ends_with('?') { " = nil" } else { "" }))).collect::<Vec<_>>().join(", ");
        if model.actions.iter().any(|action| action.name == "create" && action.durable) {
            let params = if params.is_empty() { format!("in {scope}: AccountScope") } else { format!("in {scope}: AccountScope, {params}") };
            writeln!(out, "    @concurrent public static func send({params}) async throws -> MutationReceipt<Self> {{\n        var {values}: [String: JSONValue] = [:]").unwrap();
            if !create_args.is_empty() { writeln!(out, "        var {args}: [String: JSONValue] = [:]").unwrap(); }
            for field in &create_fields {
                if create_optional(model, field) { writeln!(out, "        if let value = {} {{ {values}[\"{}\"] = try JSONValue.encode(value) }}", identifier(&field.name, true), field.name).unwrap(); }
                else { writeln!(out, "        {values}[\"{}\"] = try JSONValue.encode({})", field.name, identifier(&field.name, true)).unwrap(); }
            }
            for (name, kind) in &create_args {
                if kind.ends_with('?') { writeln!(out, "        if let value = {printed} {{ {args}[\"{name}\"] = try JSONValue.encode(value) }}", printed = identifier(name, true)).unwrap(); }
                else { writeln!(out, "        {args}[\"{name}\"] = try JSONValue.encode({printed})", printed = identifier(name, true)).unwrap(); }
            }
            writeln!(out, "        return try await {scope}.send(Self.self, values: {values}{})\n    }}", if create_args.is_empty() { String::new() } else { format!(", args: {args}") }).unwrap();
        } else {
            let params = swift_scope(&params, &scope);
            writeln!(out, "    @concurrent public static func create({params}) async throws -> MutationReceipt<Self> {{\n        var {values}: [String: JSONValue] = [:]").unwrap();
            if !create_args.is_empty() { writeln!(out, "        var {args}: [String: JSONValue] = [:]").unwrap(); }
            for field in &create_fields {
                if create_optional(model, field) { writeln!(out, "        if let value = {} {{ {values}[\"{}\"] = try JSONValue.encode(value) }}", identifier(&field.name, true), field.name).unwrap(); }
                else { writeln!(out, "        {values}[\"{}\"] = try JSONValue.encode({})", field.name, identifier(&field.name, true)).unwrap(); }
            }
            for (name, kind) in &create_args {
                if kind.ends_with('?') { writeln!(out, "        if let value = {printed} {{ {args}[\"{name}\"] = try JSONValue.encode(value) }}", printed = identifier(name, true)).unwrap(); }
                else { writeln!(out, "        {args}[\"{name}\"] = try JSONValue.encode({printed})", printed = identifier(name, true)).unwrap(); }
            }
            writeln!(out, "        return try await Self.create(values: {values}{}, in: {scope})\n    }}", if create_args.is_empty() { String::new() } else { format!(", args: {args}") }).unwrap();
        }
        let update_args = standard_args(model, "update");
        let params = fields.iter().map(|field| format!("{}: {}? = nil", identifier(&field.name, true), field_type(field, true))).chain(update_args.iter().map(|(name, kind)| format!("{}: {}? = nil", identifier(name, true), arg_type(kind, true)))).collect::<Vec<_>>().join(", ");
        let params = swift_scope(&params, &scope);
        writeln!(out, "    @concurrent public func update({params}) async throws -> MutationReceipt<Self> {{\n        var {values}: [String: JSONValue] = [:]").unwrap();
        if !update_args.is_empty() { writeln!(out, "        var {args}: [String: JSONValue] = [:]").unwrap(); }
        for field in &fields { writeln!(out, "        if let value = {} {{ {values}[\"{}\"] = try JSONValue.encode(value) }}", identifier(&field.name, true), field.name).unwrap(); }
        for (name, _) in &update_args { writeln!(out, "        if let value = {printed} {{ {args}[\"{name}\"] = try JSONValue.encode(value) }}", printed = identifier(name, true)).unwrap(); }
        writeln!(out, "        return try await self.update(values: {values}{}, in: {scope})\n    }}", if update_args.is_empty() { String::new() } else { format!(", args: {args}") }).unwrap();
        if !model.ephemeral.is_empty() {
            let params = model.ephemeral.iter().map(|field| format!("{}: {}? = nil", identifier(&field.field.name, true), field_type(&field.field, true))).collect::<Vec<_>>().join(", ");
            let params = swift_scope(&params, &scope);
            writeln!(out, "    @concurrent public func set({params}) async throws {{\n        var {values}: [String: JSONValue] = [:]").unwrap();
            for field in &model.ephemeral { writeln!(out, "        if let value = {} {{ {values}[\"{}\"] = try JSONValue.encode(value) }}", identifier(&field.field.name, true), field.field.name).unwrap(); }
            writeln!(out, "        try await self.set(values: {values}, in: {scope})\n    }}").unwrap();
        }
        if !model.associations.is_empty() {
            out.push_str("    public enum Relations {\n");
            for association in &model.associations { writeln!(out, "        public static let {} = Association<{class}, {}>(\"{}\")", identifier(&association.name, true), association.target, association.name).unwrap(); }
            out.push_str("    }\n");
        }
        for action in model.actions.iter().filter(|action| !action.upload && !["create", "update", "destroy"].contains(&action.name.as_str())) {
            let params = action.args.iter().map(|(arg, kind)| format!("{}: {}{}", identifier(arg, true), arg_type(kind, true), if kind.ends_with('?') { "? = nil" } else { "" })).collect::<Vec<_>>().join(", ");
            let literals: serde_json::Map<String, serde_json::Value> = action.optimistic.iter().filter(|(_, value)| value.get("$arg").is_none()).map(|(key, value)| (key.clone(), value.clone())).collect();
            let returns = "MutationReceipt<Self>";
            let params = swift_scope(&params, &scope);
            writeln!(out, "    @concurrent public func {}({params}) async throws -> {returns} {{", identifier(&name(&action.name), true)).unwrap();
            writeln!(out, "        {} {values} = try JSONValue.read(#\"{}\"#).objectValue", if action.optimistic.values().any(|value| value.get("$arg").is_some()) { "var" } else { "let" }, serde_json::to_string(&literals).unwrap()).unwrap();
            for (field, value) in &action.optimistic { if let Some(arg) = value.get("$arg").and_then(|arg| arg.as_str()) { writeln!(out, "        {values}[\"{field}\"] = try JSONValue.encode({printed})", printed = identifier(arg, true)).unwrap(); } }
            let args = action.args.keys().map(|arg| format!("\"{arg}\": try JSONValue.encode({printed})", printed = identifier(arg, true))).collect::<Vec<_>>().join(", ");
            writeln!(out, "        return try await self.act(\"{}\", values: {values}, args: [{}], in: {scope})\n    }}", action.name, if args.is_empty() { ":" } else { &args }).unwrap();
            if singleton_relation(model).is_some() && action.args.values().any(|kind| kind == "blob") {
                let args = action.args.keys().map(|arg| format!("{}: {}", identifier(arg, true), identifier(arg, true))).chain(std::iter::once(format!("in: {scope}"))).collect::<Vec<_>>().join(", ");
                writeln!(out, "    @concurrent public static func {}({params}) async throws -> {returns} {{\n        guard let current_ = try await Self.singleton.read(in: {scope}).first else {{ throw DecodingError.valueNotFound(Self.self, .init(codingPath: [], debugDescription: \"singleton is missing\")) }}\n        return try await current_.{}({args})\n    }}", identifier(&name(&action.name), true), identifier(&name(&action.name), true)).unwrap();
            }
        }
        for action in model.actions.iter().filter(|action| action.upload && action.batch.is_none()) {
            writeln!(out, "    @concurrent public func {}(_ request: UploadRequest, in scope_: AccountScope) async throws -> MutationReceipt<Self> {{\n        try await scope_.upload(self, action: \"{}\", request)\n    }}", identifier(&name(&action.name), true), action.name).unwrap();
        }
        out.push_str("}\n\n");
        for action in model.actions.iter().filter(|action| (action.upload && action.batch.is_none()) || !action.result.is_empty()) {
            writeln!(out, "public struct {}: Codable, Sendable {{", result_type(class, &action.name)).unwrap();
            for (field, kind) in &action.result { writeln!(out, "    public let {}: {}?", identifier(field, true), arg_type(kind, true)).unwrap(); }
            out.push_str("}\n\n");
        }
    }
    out.push_str(&edge_types);
    let spread = with_shared_in_variants(models);
    let types = declared_types(&spread);
    for declaration in &types { if matches!(declaration.kind, FieldKind::Union | FieldKind::UnionArray) { emit_swift_union(&mut out, declaration); } else { emit_swift_type(&mut out, declaration, &types, true); } }
    emit_swift_enums(&mut out, &decls);
    emit_swift_inputs(&mut out, &schema_types.inputs);
    emit_swift_operations(&mut out, operations);
    if !models.is_empty() { writeln!(out, "public let modelsJSON = #\"{}\"#", serde_json::to_string(models).unwrap()).unwrap(); }
    out
}

fn swift_scope(params: &str, scope: &str) -> String {
    if params.is_empty() { format!("in {scope}: AccountScope? = nil") } else { format!("{params}, in {scope}: AccountScope? = nil") }
}

fn kotlin_scope(params: &str, account: &str) -> String {
    if params.is_empty() { format!("{account}: AccountScope? = null") } else { format!("{params}, {account}: AccountScope? = null") }
}

pub fn emit_kotlin(models: &[ModelSpec], operations: &[OperationCall], schema_types: &SchemaTypes) -> String {
    let decls = checked_enum_decls(models, operations, schema_types);
    let one_of = if schema_types.inputs.iter().any(|input| input.one_of) { "import kotlinx.serialization.KSerializer\nimport kotlinx.serialization.SerializationException\nimport kotlinx.serialization.descriptors.SerialDescriptor\nimport kotlinx.serialization.descriptors.buildClassSerialDescriptor\nimport kotlinx.serialization.encoding.Decoder\nimport kotlinx.serialization.encoding.Encoder\n" } else { "" };
    let list_serializer = if operations.iter().any(|operation| operation.list) { "import kotlinx.serialization.builtins.ListSerializer\n" } else { "" };
    let encoding = if schema_types.inputs.is_empty() { String::new() } else { format!("import kotlinx.serialization.EncodeDefault\nimport kotlinx.serialization.ExperimentalSerializationApi\n{one_of}") };
    let mut out = format!("package com.noma4i.dblayer.models\n\nimport androidx.compose.runtime.Immutable\nimport com.noma4i.dblayer.*\n{list_serializer}{encoding}import kotlinx.serialization.SerialName\nimport kotlinx.serialization.Serializable\nimport kotlinx.serialization.json.*\n\n");
    let mut edge_types = String::new();
    for model in models {
        let class = model.key.as_str();
        let values = "values_".to_string();
        let account = ACCOUNT.to_string();
        let built = "built_".to_string();
        writeln!(out, "@Immutable\ndata class {class}(").unwrap();
        for field in model.fields.iter().chain(model.ephemeral.iter().map(|field| &field.field)) {
            writeln!(out, "    {}val {}: {}{},", if field.name == "id" { "override " } else { "" }, identifier(&field.name, false), field_type(field, false), if field.nullable || field.optional { " = null" } else { "" }).unwrap();
        }
        writeln!(out, "    override val {SYNC}: SyncInfo = SyncInfo(),\n    override val {UPLOADS}: Map<String, UploadInfo> = emptyMap()\n) : DBModel {{\n    override val {MODEL_NAME} get() = \"{class}\"\n    companion object : ModelDecoder<{class}> {{\n        override val {LAYOUT} = \"{}\"\n        override val {MODEL_NAME} = \"{class}\"\n        override fun decode(row: WireRow): {class} {{\n            val r = CellReader(row.cells)\n            return {class}(\n                id = row.id,\n{}                sync = SyncInfo.of(row.sync),\n                uploads = row.uploads.associate {{ it.action to UploadInfo.of(it.upload) }},\n            )\n        }}\n", wire::layout_of(models, model), wire::wired(model).into_iter().enumerate().map(|(at, field)| format!("                {} = {},\n", identifier(&field.name, false), wire::read_kotlin(field, at))).collect::<String>()).unwrap();
        for field in model.fields.iter().filter(|field| is_file(field)) { writeln!(out, "        val {} = FileField<{class}>(\"{}\")", identifier(&field.name, false), field.name).unwrap(); }
        for field in model.fields.iter().filter(|field| field.name != "id" && !is_file(field)) { writeln!(out, "        val {} = Column<{class}, {}>(\"{}\")", identifier(&field.name, false), column_type(field, false), field.name).unwrap(); }
        writeln!(out, "        fun where(condition: Condition) = Query(\"{class}\", this).where(condition)\n        fun ids(ids: List<String>) = idsQuery(\"{class}\", this, ids)").unwrap();
        for relation in &model.relations {
            let method = identifier(&name(&relation.name), false);
            if let Some(kind) = relation.node { kotlin_node(&mut out, class, &method, relation, kind); continue; }
            let params = relation.by_columns.iter().map(|key| format!("{}: {}", identifier(key, false), model.field(key).map(|field| field_type(field, false)).unwrap_or("String".into()))).chain(relation.text_params.iter().map(|key| text_param(relation, key, &decls, false))).collect::<Vec<_>>().join(", ");
            if params.is_empty() { writeln!(out, "        val {method}: Query<{class}> get() {{").unwrap(); }
            else { writeln!(out, "        fun {method}({params}): Query<{class}> {{").unwrap(); }
            writeln!(out, "            var {built} = Query(\"{class}\", this, relation = \"{}\", params = buildJsonObject {{ {} }})", relation.name, relation.by_columns.iter().chain(relation.text_params.iter()).map(|key| format!("this.put(\"{key}\", jsonValue({}))", identifier(key, false))).collect::<Vec<_>>().join("; ")).unwrap();
            if relation.remote.is_none() {
                for key in &relation.by_columns { writeln!(out, "            {built} = {built}.where(Condition.equal(\"{key}\", jsonValue({})))", identifier(key, false)).unwrap(); }
                if let Sort::Fields(fields) = &relation.sort { for field in fields { writeln!(out, "            {built} = {built}.order(Order(\"{}\", \"{}\"))", field.column, field.direction.name()).unwrap(); } }
            }
            writeln!(out, "            return {built}\n        }}").unwrap();
            kotlin_accessors(&mut out, &mut edge_types, model, relation, &decls);
        }
        if let Some(relation) = singleton_relation(model) {
            for action in model.actions.iter().filter(|action| !action.upload && action.args.values().any(|kind| kind == "blob")) {
                let params = action.args.iter().map(|(arg, kind)| format!("{}: {}", identifier(arg, false), arg_type(kind, false))).collect::<Vec<_>>().join(", ");
                let args = action.args.keys().map(|arg| identifier(arg, false)).chain(std::iter::once(account.clone())).collect::<Vec<_>>().join(", ");
                let params = kotlin_scope(&params, &account);
                writeln!(out, "        suspend fun {}({params}) = requireNotNull({class}.{}.read({account}).firstOrNull()) {{ \"singleton is missing\" }}.{}({args})", identifier(&name(&action.name), false), identifier(&name(&relation.name), false), identifier(&name(&action.name), false)).unwrap();
            }
        }
        for action in model.actions.iter().filter(|action| action.upload && !action.args.is_empty()) {
            let upload_args = standard_args(model, &action.name);
            let method = name(&action.name);
            let params = upload_args.iter().map(|(arg, kind)| format!("{}: {}{}", identifier(arg, false), arg_type(kind, false), if kind.ends_with('?') { "? = null" } else { "" })).collect::<Vec<_>>().join(", ");
            writeln!(out, "        fun {method}Variables({params}): JsonObject = buildJsonObject {{").unwrap();
            for (arg, kind) in &upload_args {
                if kind.ends_with('?') { writeln!(out, "            if ({printed} != null) this.put(\"{arg}\", jsonValue({printed}))", printed = identifier(arg, false)).unwrap(); }
                else { writeln!(out, "            this.put(\"{arg}\", jsonValue({printed}))", printed = identifier(arg, false)).unwrap(); }
            }
            writeln!(out, "        }}").unwrap();
        }
        let fields = writable_fields(model);
        let create_fields = create_fields(model, &fields);
        let args = "args_".to_string();
        let create_args = standard_args(model, "create");
        let params = create_fields.iter().map(|field| {
            let optional = create_optional(model, field);
            let mut kind = field_type(field, false);
            if optional && !kind.ends_with('?') { kind.push('?'); }
            format!("{}: {}{}", identifier(&field.name, false), kind, if optional { " = null" } else { "" })
        }).chain(create_args.iter().map(|(name, kind)| format!("{}: {}{}", identifier(name, false), arg_type(kind, false), if kind.ends_with('?') { "? = null" } else { "" }))).collect::<Vec<_>>().join(", ");
        if model.actions.iter().any(|action| action.name == "create" && action.durable) {
        let params = if params.is_empty() { format!("{account}: AccountScope") } else { format!("{account}: AccountScope, {params}") };
        writeln!(out, "        suspend fun send({params}): MutationReceipt<{class}> {{\n            val {values} = buildJsonObject {{").unwrap();
        for field in &create_fields {
            if create_optional(model, field) { writeln!(out, "                {}?.let {{ this.put(\"{}\", jsonValue(it)) }}", identifier(&field.name, false), field.name).unwrap(); }
            else { writeln!(out, "                this.put(\"{}\", jsonValue({}))", field.name, identifier(&field.name, false)).unwrap(); }
        }
        out.push_str("            }\n");
        if !create_args.is_empty() {
            writeln!(out, "            val {args} = buildJsonObject {{").unwrap();
            for (name, kind) in &create_args {
                if kind.ends_with('?') { writeln!(out, "                {printed}?.let {{ this.put(\"{name}\", jsonValue(it)) }}", printed = identifier(name, false)).unwrap(); }
                else { writeln!(out, "                this.put(\"{name}\", jsonValue({printed}))", printed = identifier(name, false)).unwrap(); }
            }
            out.push_str("            }\n");
        }
        writeln!(out, "            return {account}.send(this, {values}{})\n        }}", if create_args.is_empty() { String::new() } else { format!(", {args}") }).unwrap();
        } else {
            let params = create_fields.iter().map(|field| {
                let optional = create_optional(model, field);
                let mut kind = field_type(field, false);
                if optional && !kind.ends_with('?') { kind.push('?'); }
                format!("{}: {}{}", identifier(&field.name, false), kind, if optional { " = null" } else { "" })
            }).chain(create_args.iter().map(|(name, kind)| format!("{}: {}{}", identifier(name, false), arg_type(kind, false), if kind.ends_with('?') { "? = null" } else { "" }))).collect::<Vec<_>>().join(", ");
            let params = kotlin_scope(&params, &account);
            writeln!(out, "        suspend fun create({params}): MutationReceipt<{class}> = onAccount({account}) {{ database_, epoch_ ->\n            val {values} = buildJsonObject {{").unwrap();
            for field in &create_fields {
                if create_optional(model, field) { writeln!(out, "                {}?.let {{ this.put(\"{}\", jsonValue(it)) }}", identifier(&field.name, false), field.name).unwrap(); }
                else { writeln!(out, "                this.put(\"{}\", jsonValue({}))", field.name, identifier(&field.name, false)).unwrap(); }
            }
            out.push_str("            }\n");
            if !create_args.is_empty() {
                writeln!(out, "            val {args} = buildJsonObject {{").unwrap();
                for (name, kind) in &create_args {
                    if kind.ends_with('?') { writeln!(out, "                {printed}?.let {{ this.put(\"{name}\", jsonValue(it)) }}", printed = identifier(name, false)).unwrap(); }
                    else { writeln!(out, "                this.put(\"{name}\", jsonValue({printed}))", printed = identifier(name, false)).unwrap(); }
                }
                out.push_str("            }\n");
            }
            writeln!(out, "            MutationReceipt(database_, {class}, database_.create(epoch_, \"{class}\", {values}.jsonText(), {}))\n        }}", if create_args.is_empty() { "\"{}\"".to_string() } else { format!("{args}.jsonText()") }).unwrap();
        }
        out.push_str("    }\n");
        let update_args = standard_args(model, "update");
        let params = fields.iter().map(|field| kotlin_patch_param(field)).chain(update_args.iter().map(|(name, kind)| format!("{}: {}? = null", identifier(name, false), arg_type(kind, false)))).collect::<Vec<_>>().join(", ");
        let params = kotlin_scope(&params, &account);
        writeln!(out, "    suspend fun update({params}): MutationReceipt<{class}> = onAccount({account}) {{ database_, epoch_ ->\n        val {values} = buildJsonObject {{").unwrap();
        for field in &fields { writeln!(out, "{}", kotlin_patch_put(field)).unwrap(); }
        out.push_str("        }\n");
        if !update_args.is_empty() {
            writeln!(out, "        val {args} = buildJsonObject {{").unwrap();
            for (name, _) in &update_args { writeln!(out, "            {printed}?.let {{ this.put(\"{name}\", jsonValue(it)) }}", printed = identifier(name, false)).unwrap(); }
            out.push_str("        }\n");
        }
        writeln!(out, "        MutationReceipt(database_, {class}, database_.update(epoch_, this.{MODEL_NAME}, this.id, {values}.jsonText(), {}))\n    }}", if update_args.is_empty() { "\"{}\"".to_string() } else { format!("{args}.jsonText()") }).unwrap();
        writeln!(out, "    suspend fun retry(action: String?, account: AccountScope? = null): MutationReceipt<{class}> = {class}.retry(id, action, account)").unwrap();
        writeln!(out, "    suspend fun destroy(account: AccountScope? = null): MutationReceipt<{class}> = {class}.destroy(id, account)").unwrap();
        if !model.ephemeral.is_empty() {
            let params = model.ephemeral.iter().map(|field| kotlin_patch_param(&field.field)).collect::<Vec<_>>().join(", ");
            let params = kotlin_scope(&params, &account);
            writeln!(out, "    suspend fun set({params}) {{\n        onAccount({account}) {{ database_, epoch_ -> database_.setEphemeral(epoch_, this.{MODEL_NAME}, this.id, buildJsonObject {{").unwrap();
            for field in &model.ephemeral { writeln!(out, "{}", kotlin_patch_put(&field.field)).unwrap(); }
            out.push_str("        }.jsonText()) }\n    }\n");
        }
        for action in model.actions.iter().filter(|action| !action.upload && !["create", "update", "destroy"].contains(&action.name.as_str())) {
            let params = action.args.iter().map(|(arg, kind)| format!("{}: {}{}", identifier(arg, false), arg_type(kind, false), if kind.ends_with('?') { "? = null" } else { "" })).collect::<Vec<_>>().join(", ");
            let literals: serde_json::Map<String, serde_json::Value> = action.optimistic.iter().filter(|(_, value)| value.get("$arg").is_none()).map(|(key, value)| (key.clone(), value.clone())).collect();
            let from_args = action.optimistic.iter().filter_map(|(field, value)| value.get("$arg").and_then(|arg| arg.as_str()).map(|arg| format!("\"{field}\" to jsonValue({printed})", printed = identifier(arg, false)))).collect::<Vec<_>>().join(", ");
            let args = action.args.keys().map(|arg| format!("\"{arg}\" to jsonValue({printed})", printed = identifier(arg, false))).collect::<Vec<_>>().join(", ");
            let call = format!("\"{}\", JsonObject(parseJsonObject(\"\"\"{}\"\"\") + mapOf({from_args})), JsonObject(mapOf({args})), {class}", action.name, serde_json::to_string(&literals).unwrap().replace('$', "${'$'}"));
            let params = kotlin_scope(&params, &account);
            writeln!(out, "    suspend fun {}({params}): MutationReceipt<{class}> = this.act({call}, {account})", identifier(&name(&action.name), false)).unwrap();
        }
        for action in model.actions.iter().filter(|action| action.upload && action.batch.is_none()) {
            writeln!(out, "    suspend fun {}({ACCOUNT}: AccountScope, request: UploadRequest): MutationReceipt<{class}> = {ACCOUNT}.upload(this, \"{}\", request, {class})", identifier(&name(&action.name), false), action.name).unwrap();
        }
        if !model.associations.is_empty() {
            out.push_str("    object Relations {\n");
            for association in &model.associations { writeln!(out, "        val {} = Association<{class}, {}>(\"{}\", {})", identifier(&association.name, false), association.target, association.name, association.target).unwrap(); }
            out.push_str("    }\n");
        }
        out.push_str("}\n\n");
        for action in model.actions.iter().filter(|action| (action.upload && action.batch.is_none()) || !action.result.is_empty()) {
            let fields = action.result.iter().map(|(field, kind)| format!("val {}: {}? = null", identifier(field, false), arg_type(kind, false))).collect::<Vec<_>>().join(", ");
            let declaration = if fields.is_empty() { format!("class {}", result_type(class, &action.name)) } else { format!("data class {}({fields})", result_type(class, &action.name)) };
            writeln!(out, "@Immutable\n@Serializable\n{declaration}\n").unwrap();
        }
    }
    out.push_str(&edge_types);
    let spread = with_shared_in_variants(models);
    let types = declared_types(&spread);
    for declaration in &types { if matches!(declaration.kind, FieldKind::Union | FieldKind::UnionArray) { emit_kotlin_union(&mut out, declaration); } else { emit_kotlin_type(&mut out, declaration, &types, true); } }
    emit_kotlin_enums(&mut out, &decls);
    emit_kotlin_inputs(&mut out, &schema_types.inputs);
    emit_kotlin_operations(&mut out, operations);
    let models_json = serde_json::to_string(models).unwrap().replace('$', "${'$'}");
    if !models.is_empty() { writeln!(out, "val modelsJSON = \"\"\"{models_json}\"\"\"").unwrap(); }
    out
}
