use std::fs;
use std::path::PathBuf;

use crate::{compile_files, emit_kotlin, emit_swift, parse_models, Declarations};

const NESTED: &str = "model Chat do\n  attribute :title, :string\n  attribute :meta, ChatMeta, null: true do\n    attribute :tags, [:enum], values: [\"a\", \"b\"], as: MetaTag, default: [:a]\n  end\n  attribute :cards, ChatCard do\n    on CardText do\n      attribute :tones, [:enum], values: [\"calm\", \"loud\"], as: CardTone\n    end\n    shared do\n      attribute :marks, [:enum], values: [\"one\"], as: CardMark, null: true\n    end\n  end\n  attribute :ghosts, [ChatGhost] do\n    attribute :states, [:enum], values: [\"s1\", \"s2\"], as: GhostState\n  end\n  ephemeral do\n    attribute :picked, [:enum], values: [\"a\", \"b\"], default: [:a], reset_after: 5.seconds\n  end\nend\n";

#[test]
fn a_list_of_enum_values_is_typed_inside_nested_objects_union_variants_arrays_and_ephemeral_fields() {
    let models = parse_models(NESTED).unwrap();
    let swift = emit_swift(&models, &[], &Default::default());
    for line in [
        "    public let tags: [MetaTag]\n",
        "tags = try container.decodeIfPresent([MetaTag].self, forKey: .tags) ?? [MetaTag.a]",
        "tags = try r.listOpt(0, \\.asText)?.map(MetaTag.init(rawValue:)) ?? [MetaTag.a]",
        "    public let tones: [CardTone]\n",
        "tones = try r.list(1, \\.asText).map(CardTone.init(rawValue:))",
        "    public let marks: [CardMark]?\n",
        "    public let states: [GhostState]\n",
        "states = try r.list(0, \\.asText).map(GhostState.init(rawValue:))",
        "self.picked = try r.listOpt(4, \\.asText)?.map(ChatPicked.init(rawValue:)) ?? [ChatPicked.a]",
        "public enum MetaTag: Codable, Sendable, Hashable {",
        "public enum GhostState: Codable, Sendable, Hashable {",
    ] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&models, &[], &Default::default());
    for line in [
        "val tags: List<MetaTag> = listOf(MetaTag.A),",
        "val tones: List<CardTone>,",
        "val marks: List<CardMark>? = null,",
        "val states: List<GhostState>,",
        "picked = r.listOpt(4, Cell::asText)?.map { ChatPicked.of(it) } ?: listOf(ChatPicked.A),",
        "sealed class CardTone(override val raw: String) : RawEnum {",
    ] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
}

#[test]
fn an_enum_default_outside_its_values_is_an_error_with_a_line_at_every_position() {
    for (case, source) in [
        ("model enum", "model Chat do\n  attribute :kind, :enum, values: [\"A\", \"B\"], as: ChatKind, default: :BAD\nend\n".to_string()),
        ("model enum list", "model Chat do\n  attribute :lists, [:enum], values: [\"A\", \"B\"], as: ChatList, default: [:A, :BAD]\nend\n".to_string()),
        ("nested enum list", NESTED.replace("default: [:a]\n  end\n  attribute :cards", "default: [:zzz]\n  end\n  attribute :cards")),
    ] {
        let errors = parse_models(&source).expect_err(case);
        assert!(errors.iter().any(|found| found.line > 0 && found.message.contains("default must match its type")), "{case}: {errors:?}");
    }
}

const SCHEMA: &str = "type Query { a: Int }\ntype Mutation { widgetsSend(input: EmojiInput!): Reply }\nenum WidgetKind { EMOJI GIF }\ninput EmojiInput { code: String!, kinds: [WidgetKind!] }\ntype Reply { kinds: [WidgetKind!]!, nested: Inner }\ntype Inner { many: [WidgetKind!] }\n";
const OPERATION: &str = "mutation WidgetsSend($input: EmojiInput!) { widgetsSend(input: $input) { kinds nested { many } } }\n";
const CALL: &str = "operation :widgets_send do\n  mutation \"WidgetsSend\"\n  parameter :input, EmojiInput\n  returns do\n    attribute :kinds, [WidgetKind]\n    attribute :nested, WidgetsSendResultNested do\n      attribute :many, [WidgetKind]\n    end\n  end\nend\n";

fn compile(source: &str, schema: Option<&str>) -> Result<Declarations, Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("calls.dbl");
    fs::write(&file, source).unwrap();
    let schema_file = schema.map(|text| {
        let path = directory.path().join("schema.graphql");
        fs::write(&path, text).unwrap();
        fs::create_dir(directory.path().join("mutations")).unwrap();
        fs::write(directory.path().join("mutations/send.graphql"), OPERATION).unwrap();
        path
    });
    let documents: Vec<PathBuf> = schema_file.iter().map(|_| directory.path().join("mutations")).collect();
    compile_files(&[file], schema_file.as_deref(), &documents).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

#[test]
fn an_operation_takes_a_list_of_schema_enums_in_its_input_object_and_its_result() {
    let declared = compile(CALL, Some(SCHEMA)).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &declared.inputs);
    for line in [
        "public let kinds: GraphQLNullable<[WidgetKind]>",
        "    public let kinds: [WidgetKind]?\n",
        "kinds = try container.decodeIfPresent([WidgetKind].self, forKey: .kinds)",
        "    public let many: [WidgetKind]?\n",
        "public enum WidgetKind: Codable, Sendable, Hashable {",
    ] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &declared.inputs);
    for line in ["val kinds: GraphQLNullable<List<WidgetKind>> = GraphQLNullable.Omitted", "    val kinds: List<WidgetKind>? = null,", "    val many: List<WidgetKind>? = null,", "sealed class WidgetKind(override val raw: String) : RawEnum {"] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
    let without = compile(CALL, None).unwrap_err();
    assert!(without.iter().any(|text| text.contains("--schema")), "{without:?}");
    let wrong = compile(&CALL.replace("attribute :kinds, [WidgetKind]", "attribute :kinds, [EmojiInput]"), Some(SCHEMA)).unwrap_err();
    assert!(wrong.iter().any(|text| text.contains("`EmojiInput`") && text.contains("not an enum")), "{wrong:?}");
}
