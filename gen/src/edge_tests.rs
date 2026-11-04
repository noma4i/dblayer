use crate::{emit_kotlin, emit_swift, parse_models, SchemaTypes};

const SOURCE: &str = "model Moment do\n  attribute :text, :string, null: true\n  scope :feed do\n    query \"MomentsFeed\"\n    paginate size: 20, direction: :forward\n    edge do\n      attribute :impressionUuid, :string, null: true\n      attribute :impressionScore, :int, null: true\n    end\n  end\n  scope :vibe_feed do\n    parameter :vibeId, :string\n    query \"MomentsFeed\"\n    paginate size: 20, direction: :forward\n    edge do\n      attribute :impressionUuid, :string, null: true\n    end\n  end\n  scope :recent do\n    order text: :desc\n  end\nend\n";

#[test]
fn edge_values_are_a_typed_read_per_scope_and_not_a_property_of_the_row() {
    let models = parse_models(SOURCE).unwrap();
    let swift = emit_swift(&models, &[], &SchemaTypes::default());
    let kotlin = emit_kotlin(&models, &[], &SchemaTypes::default());
    for needle in [
        "@concurrent public static func feedEdge(of id: String, in scope_: AccountScope? = nil) async throws -> MomentFeedEdge?",
        "@concurrent public static func vibeFeedEdge(_ vibeId: String, of id: String, in scope_: AccountScope? = nil) async throws -> MomentVibeFeedEdge?",
        "public struct MomentFeedEdge: Sendable, Equatable {\n    public let impressionUuid: String?\n    public let impressionScore: Int?",
        "self.impressionScore = values[\"impressionScore\"]?.intValue",
        "public struct MomentVibeFeedEdge",
    ] {
        assert!(swift.contains(needle), "swift lacks {needle}");
    }
    for needle in [
        "suspend fun feedEdge(id: String, account_: AccountScope? = null): MomentFeedEdge? = Moment.feed.edgeValues(id, account_)?.let { MomentFeedEdge(it) }",
        "suspend fun vibeFeedEdge(vibeId: String, id: String, account_: AccountScope? = null): MomentVibeFeedEdge?",
        "data class MomentFeedEdge(\n    val impressionUuid: String? = null,\n    val impressionScore: Long? = null\n)",
        "(values[\"impressionScore\"] as? JsonPrimitive)?.longOrNull",
    ] {
        assert!(kotlin.contains(needle), "kotlin lacks {needle}");
    }
    assert!(!swift.contains("public let impressionUuid: String?\n    public let sync") && !kotlin.contains("val impressionUuid: String?,\n    override val sync"), "no column on the row");
    assert!(!swift.contains("Edge(") || !swift.contains("recentEdge"), "a scope without edges gets no accessor");
}
