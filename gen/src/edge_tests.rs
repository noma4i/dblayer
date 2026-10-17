use crate::{emit_kotlin, emit_swift, parse_models, SchemaTypes};

const SOURCE: &str = "model Moment do\n  field :text, type: :string, null: true\n  scope :feed do\n    fetch MomentsFeed\n    paginate 20\n    edge :impressionUuid, type: :string, null: true\n    edge :impressionScore, type: :int, null: true\n  end\n  scope :vibe_feed, text: [:vibeId] do\n    fetch MomentsFeed\n    paginate 20\n    edge :impressionUuid, type: :string, null: true\n  end\n  scope :recent do\n    order text: :desc\n  end\nend\n";

#[test]
fn edge_values_are_a_typed_read_per_scope_and_not_a_property_of_the_row() {
    let models = parse_models(SOURCE).unwrap();
    let swift = emit_swift(&models, &[], &SchemaTypes::default());
    let kotlin = emit_kotlin(&models, &[], &SchemaTypes::default());
    for needle in [
        "public static func feedEdge(of id: String) throws -> MomentFeedEdge?",
        "public static func readFeedEdge(of id: String) async throws -> MomentFeedEdge?",
        "public static func vibeFeedEdge(_ vibeId: String, of id: String) throws -> MomentVibeFeedEdge?",
        "public struct MomentFeedEdge: Sendable, Equatable {\n    public let impressionUuid: String?\n    public let impressionScore: Int?",
        "self.impressionScore = values[\"impressionScore\"]?.intValue",
        "public struct MomentVibeFeedEdge",
    ] {
        assert!(swift.contains(needle), "swift lacks {needle}");
    }
    for needle in [
        "fun feedEdge(id: String): MomentFeedEdge? = feed.edgeValues(id)?.let { MomentFeedEdge(it) }",
        "suspend fun readFeedEdge(id: String): MomentFeedEdge?",
        "fun vibeFeedEdge(vibeId: String, id: String): MomentVibeFeedEdge?",
        "data class MomentFeedEdge(\n    val impressionUuid: String? = null,\n    val impressionScore: Long? = null\n)",
        "(values[\"impressionScore\"] as? JsonPrimitive)?.longOrNull",
    ] {
        assert!(kotlin.contains(needle), "kotlin lacks {needle}");
    }
    assert!(!swift.contains("public let impressionUuid: String?\n    public let sync") && !kotlin.contains("val impressionUuid: String?,\n    override val sync"), "no column on the row");
    assert!(!swift.contains("Edge(") || !swift.contains("recentEdge"), "a scope without edges gets no accessor");
}
