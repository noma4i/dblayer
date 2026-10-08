use dblayer::schema::{FieldKind, FieldSpec, ModelSpec, Sort};
use dblayer::value::{ScalarKind, SortDirection};
use std::fmt::Write;

fn name(text: &str) -> String {
    let upper = crate::build::camel(text);
    let mut chars = upper.chars();
    chars.next().map(|first| first.to_lowercase().collect::<String>() + chars.as_str()).unwrap_or_default()
}

fn scalar(kind: ScalarKind, swift: bool) -> &'static str {
    match (kind, swift) {
        (ScalarKind::Bool, _) => "Bool",
        (ScalarKind::Int, true) => "Int64",
        (ScalarKind::Int, false) => "Long",
        (ScalarKind::Num, _) => "Double",
        _ => "String",
    }
}

fn field_type(field: &FieldSpec, swift: bool) -> String {
    let base = match &field.kind {
        FieldKind::Scalar { kind } => scalar(*kind, swift).to_string(),
        FieldKind::Enum { .. } => "String".into(),
        FieldKind::List { element } => if swift { format!("[{}]", scalar(*element, true)) } else { format!("List<{}>", scalar(*element, false)) },
        _ => field.app_type.clone().unwrap_or_else(|| if swift { "JSONValue".into() } else { "JsonElement".into() }),
    };
    let base = if !swift && base == "Bool" { "Boolean".into() } else { base };
    if field.nullable || field.optional { format!("{base}?") } else { base }
}

pub fn emit_swift(models: &[ModelSpec]) -> String {
    let mut out = "import Foundation\nimport DBlayer\n\n".to_string();
    for model in models {
        let class = model.key.as_str();
        writeln!(out, "public struct {class}: DBModel {{\n    public static let modelName = \"{class}\"").unwrap();
        for field in model.fields.iter().chain(model.ephemeral.iter().map(|field| &field.field)) {
            writeln!(out, "    public let {}: {}", field.name, field_type(field, true)).unwrap();
        }
        writeln!(out, "    public let sync: SyncInfo\n    public let upload: UploadInfo?\n").unwrap();
        for field in &model.fields {
            if field.name != "id" { writeln!(out, "    public static let {} = Column<{class}, {}>(\"{}\")", field.name, field_type(field, true), field.name).unwrap(); }
        }
        out.push_str("    public static func find(_ id: String) -> LiveSource<Self?> { Query<Self>().find(id) }\n    public static func `where`(_ condition: Condition) -> Query<Self> { Query<Self>().where(condition) }\n");
        for relation in &model.relations {
            let method = name(&relation.name);
            if relation.by_columns.is_empty() { writeln!(out, "    public static var {method}: Query<Self> {{").unwrap(); }
            else {
                let params = relation.by_columns.iter().map(|key| format!("_ {key}: {}", model.field(key).map(|field| field_type(field, true)).unwrap_or("String".into()))).collect::<Vec<_>>().join(", ");
                writeln!(out, "    public static func {method}({params}) -> Query<Self> {{").unwrap();
            }
            let params = relation.by_columns.iter().map(|key| format!("\"{key}\": JSONValue.encode({key})")).collect::<Vec<_>>().join(", ");
            writeln!(out, "        var query = Query<Self>(relation: \"{}\", params: [{}])", relation.name, if params.is_empty() { ":" } else { &params }).unwrap();
            if relation.remote.is_none() {
                for key in &relation.by_columns { writeln!(out, "        query = query.where(Condition.equal(\"{key}\", JSONValue.encode({key})))").unwrap(); }
                if let Sort::Fields(fields) = &relation.sort {
                    for field in fields { writeln!(out, "        query = query.order(Order(field: \"{}\", direction: \"{}\"))", field.column, if field.direction == SortDirection::Desc { "desc" } else { "asc" }).unwrap(); }
                }
            }
            out.push_str("        return query\n    }\n");
        }
        let fields: Vec<_> = model.fields.iter().filter(|field| field.name != "id").collect();
        let params = fields.iter().map(|field| {
            let optional = field.nullable || field.optional || field.default.is_some();
            let mut kind = field_type(field, true);
            if optional && !kind.ends_with('?') { kind.push('?'); }
            format!("{}: {}{}", field.name, kind, if optional { " = nil" } else { "" })
        }).collect::<Vec<_>>().join(", ");
        writeln!(out, "    public static func create({params}) async throws -> Self {{\n        var values: [String: JSONValue] = [:]").unwrap();
        for field in &fields {
            if field.nullable || field.optional || field.default.is_some() { writeln!(out, "        if let value = {} {{ values[\"{}\"] = JSONValue.encode(value) }}", field.name, field.name).unwrap(); }
            else { writeln!(out, "        values[\"{}\"] = JSONValue.encode({})", field.name, field.name).unwrap(); }
        }
        out.push_str("        return try await create(values: values)\n    }\n");
        let params = fields.iter().map(|field| format!("{}: {}{} = nil", field.name, field_type(field, true), if field.nullable || field.optional { "?" } else { "?" })).collect::<Vec<_>>().join(", ");
        writeln!(out, "    public func update({params}) async throws -> Self {{\n        var values: [String: JSONValue] = [:]").unwrap();
        for field in &fields { writeln!(out, "        if let value = {} {{ values[\"{}\"] = JSONValue.encode(value) }}", field.name, field.name).unwrap(); }
        out.push_str("        return try await update(values: values)\n    }\n");
        if !model.ephemeral.is_empty() {
            let params = model.ephemeral.iter().map(|field| format!("{}: {}? = nil", field.field.name, field_type(&field.field, true))).collect::<Vec<_>>().join(", ");
            writeln!(out, "    public func set({params}) async throws {{\n        var values: [String: JSONValue] = [:]").unwrap();
            for field in &model.ephemeral { writeln!(out, "        if let value = {} {{ values[\"{}\"] = JSONValue.encode(value) }}", field.field.name, field.field.name).unwrap(); }
            out.push_str("        try await set(values: values)\n    }\n");
        }
        for association in &model.associations {
            let target = association.target.as_str();
            writeln!(out, "    public var {}: LiveSource<[{}]> {{ related(\"{}\", as: {}.self) }}", association.name, target, association.name, target).unwrap();
        }
        for action in model.actions.iter().filter(|action| !["create", "update", "destroy"].contains(&action.name.as_str())) {
            writeln!(out, "    public func {}() async throws -> Self {{ try await act(\"{}\", values: JSONValue.parse(#\"{}\"#).objectValue, args: [:]) }}", name(&action.name), action.name, serde_json::to_string(&action.optimistic).unwrap()).unwrap();
        }
        out.push_str("}\n\n");
    }
    writeln!(out, "public let modelsJSON = #\"{}\"#", serde_json::to_string(models).unwrap()).unwrap();
    out
}

pub fn emit_kotlin(models: &[ModelSpec]) -> String {
    let mut out = "package com.noma4i.dblayer.models\n\nimport com.noma4i.dblayer.*\nimport kotlinx.serialization.Serializable\nimport kotlinx.serialization.json.*\n\n".to_string();
    for model in models {
        let class = model.key.as_str();
        writeln!(out, "@Serializable\ndata class {class}(").unwrap();
        for field in model.fields.iter().chain(model.ephemeral.iter().map(|field| &field.field)) {
            writeln!(out, "    {}val {}: {}{},", if field.name == "id" { "override " } else { "" }, field.name, field_type(field, false), if field.nullable || field.optional { " = null" } else { "" }).unwrap();
        }
        writeln!(out, "    override val sync: SyncInfo = SyncInfo(),\n    override val upload: UploadInfo? = null\n) : DBModel {{\n    override val modelName get() = \"{class}\"\n    companion object {{").unwrap();
        for field in model.fields.iter().filter(|field| field.name != "id") { writeln!(out, "        val {} = Column<{class}, {}>(\"{}\")", field.name, field_type(field, false), field.name).unwrap(); }
        writeln!(out, "        fun find(id: String) = Query(\"{class}\", serializer()).find(id)\n        fun where(condition: Condition) = Query(\"{class}\", serializer()).where(condition)").unwrap();
        for relation in &model.relations {
            let method = name(&relation.name);
            let params = relation.by_columns.iter().map(|key| format!("{key}: {}", model.field(key).map(|field| field_type(field, false)).unwrap_or("String".into()))).collect::<Vec<_>>().join(", ");
            if params.is_empty() { writeln!(out, "        val {method}: Query<{class}> get() {{").unwrap(); }
            else { writeln!(out, "        fun {method}({params}): Query<{class}> {{").unwrap(); }
            let params = relation.by_columns.iter().map(|key| format!("\"{key}\" to jsonValue({key})")).collect::<Vec<_>>().join(", ");
            writeln!(out, "            var query = Query(\"{class}\", serializer(), relation = \"{}\", params = buildJsonObject {{ {} }})", relation.name, relation.by_columns.iter().map(|key| format!("put(\"{key}\", jsonValue({key}))")).collect::<Vec<_>>().join("; ")).unwrap();
            let _ = params;
            if relation.remote.is_none() {
                for key in &relation.by_columns { writeln!(out, "            query = query.where(Condition.equal(\"{key}\", jsonValue({key})))").unwrap(); }
                if let Sort::Fields(fields) = &relation.sort { for field in fields { writeln!(out, "            query = query.order(Order(\"{}\", \"{}\"))", field.column, if field.direction == SortDirection::Desc { "desc" } else { "asc" }).unwrap(); } }
            }
            out.push_str("            return query\n        }\n");
        }
        let fields: Vec<_> = model.fields.iter().filter(|field| field.name != "id").collect();
        let params = fields.iter().map(|field| {
            let optional = field.nullable || field.optional || field.default.is_some();
            let mut kind = field_type(field, false);
            if optional && !kind.ends_with('?') { kind.push('?'); }
            format!("{}: {}{}", field.name, kind, if optional { " = null" } else { "" })
        }).collect::<Vec<_>>().join(", ");
        writeln!(out, "        suspend fun create({params}): {class} {{\n            val values = buildJsonObject {{").unwrap();
        for field in &fields {
            if field.nullable || field.optional || field.default.is_some() { writeln!(out, "                {}?.let {{ put(\"{}\", jsonValue(it)) }}", field.name, field.name).unwrap(); }
            else { writeln!(out, "                put(\"{}\", jsonValue({}))", field.name, field.name).unwrap(); }
        }
        writeln!(out, "            }}\n            return dbJson.decodeFromString(serializer(), DBlayer.database.create(\"{class}\", values.toString()))\n        }}\n    }}").unwrap();
        let params = fields.iter().map(|field| format!("{}: {}{} = null", field.name, field_type(field, false), if field.nullable || field.optional { "" } else { "?" })).collect::<Vec<_>>().join(", ");
        writeln!(out, "    suspend fun update({params}): {class} {{\n        val values = buildJsonObject {{").unwrap();
        for field in &fields { writeln!(out, "            {}?.let {{ put(\"{}\", jsonValue(it)) }}", field.name, field.name).unwrap(); }
        writeln!(out, "        }}\n        return dbJson.decodeFromString(serializer(), DBlayer.database.update(modelName, id, values.toString()))\n    }}").unwrap();
        if !model.ephemeral.is_empty() {
            let params = model.ephemeral.iter().map(|field| format!("{}: {}{} = null", field.field.name, field_type(&field.field, false), if field.field.nullable || field.field.optional { "" } else { "?" })).collect::<Vec<_>>().join(", ");
            writeln!(out, "    suspend fun set({params}) {{\n        DBlayer.database.setEphemeral(modelName, id, buildJsonObject {{").unwrap();
            for field in &model.ephemeral { writeln!(out, "            {}?.let {{ put(\"{}\", jsonValue(it)) }}", field.field.name, field.field.name).unwrap(); }
            out.push_str("        }.toString())\n    }\n");
        }
        for action in model.actions.iter().filter(|action| !["create", "update", "destroy"].contains(&action.name.as_str())) {
            writeln!(out, "    suspend fun {}(): {class} = act(\"{}\", parseJsonObject(\"\"\"{}\"\"\"), JsonObject(emptyMap()), serializer())", name(&action.name), action.name, serde_json::to_string(&action.optimistic).unwrap()).unwrap();
        }
        for association in &model.associations { writeln!(out, "    val {} get() = related(\"{}\", {}.serializer())", association.name, association.name, association.target).unwrap(); }
        out.push_str("}\n\n");
    }
    writeln!(out, "val modelsJSON = \"\"\"{}\"\"\"", serde_json::to_string(models).unwrap()).unwrap();
    out
}
