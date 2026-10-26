use dblayer::schema::{NodeRead, RelationSpec};

pub(super) fn swift_node(out: &mut String, method: &str, relation: &RelationSpec, kind: NodeRead) {
    let relation = &relation.name;
    match kind {
        NodeRead::One => out.push_str(&format!("    @concurrent public static func {method}(_ id: String, force: Bool = false) async throws -> Self? {{\n        try await readNode(relation: \"{relation}\", id: id, force: force)\n    }}\n    @MainActor public static func entity(_ id: String, fetch: EntityFetch) -> Entity<Self> {{ Entities<Self>.shared.entity(id, fetch: fetch) }}\n")),
        NodeRead::Many => out.push_str(&format!("    @concurrent public static func {method}(_ ids: [String]) async throws -> [Self] {{\n        try await readNodes(relation: \"{relation}\", ids: ids)\n    }}\n")),
    }
}

pub(super) fn kotlin_node(out: &mut String, class: &str, method: &str, relation: &RelationSpec, kind: NodeRead) {
    let relation = &relation.name;
    match kind {
        NodeRead::One => out.push_str(&format!("        suspend fun {method}(id: String, force: Boolean = false): {class}? = offCaller {{ DBlayer.database.node(\"{class}\", \"{relation}\", id, force)?.let {{ read(it) }} }}\n        fun entity(id: String, fetch: EntityFetch) = entityOf(\"{class}\", this, id, fetch)\n")),
        NodeRead::Many => out.push_str(&format!("        suspend fun {method}(ids: List<String>): List<{class}> = offCaller {{ DBlayer.database.nodes(\"{class}\", \"{relation}\", ids).map {{ read(it) }} }}\n")),
    }
}
