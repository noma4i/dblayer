use dblayer::schema::{NodeRead, RelationSpec};

pub(super) fn swift_node(out: &mut String, method: &str, relation: &RelationSpec, kind: NodeRead) {
    let relation = &relation.name;
    match kind {
        NodeRead::One => out.push_str(&format!("    nonisolated(nonsending) public static func {method}(_ id: String, force: Bool = false, in scope: AccountScope? = nil) async throws -> Self? {{\n        try await readNode(relation: \"{relation}\", id: id, force: force, in: scope)\n    }}\n")),
        NodeRead::Many => out.push_str(&format!("    nonisolated(nonsending) public static func {method}(_ ids: [String], in scope: AccountScope? = nil) async throws -> [Self] {{\n        try await readNodes(relation: \"{relation}\", ids: ids, in: scope)\n    }}\n")),
    }
}

pub(super) fn kotlin_node(out: &mut String, class: &str, method: &str, relation: &RelationSpec, kind: NodeRead) {
    let relation = &relation.name;
    match kind {
        NodeRead::One => out.push_str(&format!("        suspend fun {method}(id: String, force: Boolean = false, account: AccountScope? = null): {class}? = onAccountOf(account) {{ _, ordered_, epoch_ -> ordered_.node(epoch_, \"{class}\", \"{relation}\", id, force)?.let {{ {class}.read(it) }} }}\n")),
        NodeRead::Many => out.push_str(&format!("        suspend fun {method}(ids: List<String>, account: AccountScope? = null): List<{class}> = onAccountOf(account) {{ _, ordered_, epoch_ -> ordered_.nodes(epoch_, \"{class}\", \"{relation}\", ids).map {{ {class}.read(it) }} }}\n")),
    }
}
