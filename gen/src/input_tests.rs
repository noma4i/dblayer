use std::fs;

use crate::{emit_kotlin, emit_swift, load_input_objects, parse_all, InputField};

const SCHEMA: &str = "type Query { a: Int }\nscalar DateTime\nenum Mood { HAPPY SAD }\ninput StickerInput { name: String!, mood: Mood, tags: [String!]!, notes: [String], size: Int, scale: Float, pinned: Boolean!, sentAt: DateTime, owner: OwnerInput!, helpers: [OwnerInput!] }\ninput OwnerInput { id: ID!, nickname: String }\ninput Unused { x: Int }\n";
const SEND: &str = "operation :sticker_send do\n  mutation StickerSend\n  args input: :StickerInput\n  result id: :string\nend\n";

fn field(name: &str, base: &str, list: bool, item_optional: bool, optional: bool) -> InputField {
    InputField { name: name.into(), base: base.into(), list, item_optional, optional }
}

fn read(source: &str) -> Result<Vec<crate::InputObject>, String> {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("schema.graphql");
    fs::write(&path, SCHEMA).unwrap();
    let declared = parse_all(&[source]).unwrap();
    load_input_objects(Some(&path), &declared.operations)
}

#[test]
fn the_input_objects_an_operation_names_are_read_with_everything_they_nest() {
    let objects = read(SEND).unwrap();
    assert_eq!(objects.iter().map(|object| object.name.as_str()).collect::<Vec<_>>(), vec!["StickerInput", "OwnerInput"]);
    assert_eq!(
        objects[0].fields,
        vec![
            field("name", "string", false, false, false),
            field("mood", "string", false, false, true),
            field("tags", "string", true, false, false),
            field("notes", "string", true, true, true),
            field("size", "int", false, false, true),
            field("scale", "num", false, false, true),
            field("pinned", "bool", false, false, false),
            field("sentAt", "string", false, false, true),
            field("owner", "OwnerInput", false, false, false),
            field("helpers", "OwnerInput", true, false, true),
        ]
    );
    assert_eq!(objects[1].fields, vec![field("id", "string", false, false, false), field("nickname", "string", false, false, true)]);
}

#[test]
fn a_name_that_is_not_an_input_object_of_the_schema_fails_and_no_schema_fails_too() {
    for (case, source, expected) in [
        ("unknown name", "operation :a do\n  mutation A\n  args input: :Nope\nend\n", "`Nope` is not an input object of the schema"),
        ("an enum", "operation :a do\n  mutation A\n  args input: :Mood\nend\n", "`Mood` is not an input object of the schema"),
    ] {
        assert_eq!(read(source).unwrap_err(), expected, "{case}");
    }
    let declared = parse_all(&[SEND]).unwrap();
    let error = load_input_objects(None, &declared.operations).unwrap_err();
    assert!(error.contains("`StickerInput`") && error.contains("--schema"), "{error}");
    assert!(load_input_objects(None, &parse_all(&["operation :out do\n  mutation Out\nend\n"]).unwrap().operations).unwrap().is_empty());
}

#[test]
fn input_objects_become_structs_and_the_call_takes_them_on_both_platforms() {
    let declared = parse_all(&[SEND]).unwrap();
    let inputs = read(SEND).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &inputs);
    for line in [
        "public struct StickerInput: Encodable, Sendable {",
        "    public let tags: [String]",
        "    public let notes: [String?]?",
        "    public let scale: Double?",
        "    public let owner: OwnerInput",
        "    public let helpers: [OwnerInput]?",
        "    public init(name: String, mood: String? = nil, tags: [String], notes: [String?]? = nil, size: Int? = nil, scale: Double? = nil, pinned: Bool, sentAt: String? = nil, owner: OwnerInput, helpers: [OwnerInput]? = nil) {",
        "public struct OwnerInput: Encodable, Sendable {",
        "public static func stickerSend(input: StickerInput) async throws -> StickerSendResult {",
        "variables[\"input\"] = JSONValue.encode(input)",
    ] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &inputs);
    for line in [
        "@OptIn(ExperimentalSerializationApi::class)\n@Serializable\ndata class StickerInput(val name: String, @EncodeDefault(EncodeDefault.Mode.NEVER) val mood: String? = null, val tags: List<String>, @EncodeDefault(EncodeDefault.Mode.NEVER) val notes: List<String?>? = null, @EncodeDefault(EncodeDefault.Mode.NEVER) val size: Int? = null, @EncodeDefault(EncodeDefault.Mode.NEVER) val scale: Double? = null, val pinned: Boolean, @EncodeDefault(EncodeDefault.Mode.NEVER) val sentAt: String? = null, val owner: OwnerInput, @EncodeDefault(EncodeDefault.Mode.NEVER) val helpers: List<OwnerInput>? = null)",
        "@OptIn(ExperimentalSerializationApi::class)\n@Serializable\ndata class OwnerInput(val id: String, @EncodeDefault(EncodeDefault.Mode.NEVER) val nickname: String? = null)",
        "import kotlinx.serialization.EncodeDefault\nimport kotlinx.serialization.ExperimentalSerializationApi\n",
        "suspend fun stickerSend(input: StickerInput): StickerSendResult {",
        "variables[\"input\"] = jsonValue(input)",
    ] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
}
