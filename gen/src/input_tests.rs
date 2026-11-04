use std::fs;

use crate::{emit_kotlin, emit_swift, load_schema_types, parse_all, SchemaTypes, InputField, Ty, TyKind};

const SCHEMA: &str = "type Query { a: Int }\nscalar DateTime\nenum Mood { HAPPY SAD }\ninput StickerInput { name: String!, mood: Mood, tags: [String!]!, notes: [String], size: Int, scale: Float, pinned: Boolean!, sentAt: DateTime, owner: OwnerInput!, helpers: [OwnerInput!] }\ninput OwnerInput { id: ID!, nickname: String }\ninput Unused { x: Int }\n";
const SEND: &str = "scalar :DateTime, as: :date\n\noperation :sticker_send do\n  mutation \"StickerSend\"\n  parameter :input, StickerInput\n  returns do\n    attribute :id, :string\n  end\nend\n";

fn field(name: &str, base: &str, list: bool, item_optional: bool, optional: bool) -> InputField {
    let scalar = Ty { kind: TyKind::Named(base.into()), nullable: if list { item_optional } else { optional }, has_default: false };
    let ty = if list { Ty { kind: TyKind::List(Box::new(scalar)), nullable: optional, has_default: false } } else { scalar };
    InputField { name: name.into(), ty }
}

fn read(source: &str) -> Result<SchemaTypes, String> {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("schema.graphql");
    fs::write(&path, SCHEMA).unwrap();
    let declared = parse_all(&[source]).unwrap();
    load_schema_types(Some(&path), &declared.scalars, &declared.models, &declared.operations)
}

#[test]
fn the_input_objects_an_operation_names_are_read_with_everything_they_nest() {
    let objects = read(SEND).unwrap().inputs;
    assert_eq!(objects.iter().map(|object| object.name.as_str()).collect::<Vec<_>>(), vec!["StickerInput", "OwnerInput"]);
    assert_eq!(
        objects[0].fields,
        vec![
            field("name", "string", false, false, false),
            field("mood", "Mood", false, false, true),
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
    let source = "operation :a do\n  mutation \"A\"\n  parameter :input, Nope\nend\n";
    assert_eq!(read(source).unwrap_err(), "`Nope` is not an input object or enum of the schema", "unknown name");
    let declared = parse_all(&[SEND]).unwrap();
    let error = load_schema_types(None, &declared.scalars, &declared.models, &declared.operations).unwrap_err();
    assert!(error.contains("`StickerInput`") && error.contains("--schema"), "{error}");
    assert_eq!(load_schema_types(None, &Default::default(), &[], &parse_all(&["operation :out do\n  mutation \"Out\"\nend\n"]).unwrap().operations).unwrap(), SchemaTypes::default());
}

#[test]
fn input_objects_become_structs_and_the_call_takes_them_on_both_platforms() {
    let declared = parse_all(&[SEND]).unwrap();
    let inputs = read(SEND).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &inputs);
    for line in [
        "public struct StickerInput: Encodable, Sendable, Equatable {",
        "    public let tags: [String]",
        "    public let notes: GraphQLNullable<[String?]>",
        "    public let scale: GraphQLNullable<Double>",
        "    public let owner: OwnerInput",
        "    public let helpers: GraphQLNullable<[OwnerInput]>",
        "    public init(name: String, mood: GraphQLNullable<Mood> = .omitted, tags: [String], notes: GraphQLNullable<[String?]> = .omitted, size: GraphQLNullable<Int> = .omitted, scale: GraphQLNullable<Double> = .omitted, pinned: Bool, sentAt: GraphQLNullable<String> = .omitted, owner: OwnerInput, helpers: GraphQLNullable<[OwnerInput]> = .omitted) {",
        "public struct OwnerInput: Encodable, Sendable, Equatable {",
        "public static func stickerSend(input: StickerInput, in scope_: AccountScope? = nil) async throws -> StickerSendResult {",
        "variables_[\"input\"] = try JSONValue.encode(input)",
    ] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &inputs);
    for line in [
        "@OptIn(ExperimentalSerializationApi::class)\n@Serializable\ndata class StickerInput(val name: String, @EncodeDefault(EncodeDefault.Mode.NEVER) val mood: GraphQLNullable<Mood> = GraphQLNullable.Omitted, val tags: List<String>, @EncodeDefault(EncodeDefault.Mode.NEVER) val notes: GraphQLNullable<List<String?>> = GraphQLNullable.Omitted, @EncodeDefault(EncodeDefault.Mode.NEVER) val size: GraphQLNullable<Long> = GraphQLNullable.Omitted, @EncodeDefault(EncodeDefault.Mode.NEVER) val scale: GraphQLNullable<Double> = GraphQLNullable.Omitted, val pinned: Boolean, @EncodeDefault(EncodeDefault.Mode.NEVER) val sentAt: GraphQLNullable<String> = GraphQLNullable.Omitted, val owner: OwnerInput, @EncodeDefault(EncodeDefault.Mode.NEVER) val helpers: GraphQLNullable<List<OwnerInput>> = GraphQLNullable.Omitted)",
        "@OptIn(ExperimentalSerializationApi::class)\n@Serializable\ndata class OwnerInput(val id: String, @EncodeDefault(EncodeDefault.Mode.NEVER) val nickname: GraphQLNullable<String> = GraphQLNullable.Omitted)",
        "import kotlinx.serialization.EncodeDefault\nimport kotlinx.serialization.ExperimentalSerializationApi\n",
        "suspend fun stickerSend(input: StickerInput, account: AccountScope? = null): StickerSendResult {",
        "variables_[\"input\"] = jsonValue(input)",
    ] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
}

const MOMENT_SCHEMA: &str = "type Query { a: Int }\nscalar JSON\nenum Kind { EMOJI GIF }\ninput PointInput { x: Float, y: Float }\ninput SettingsInput { itemProps: JSON!, extras: [JSON!], offset: PointInput, rotate: Float }\ninput WidgetInput { kind: Kind!, settings: SettingsInput! }\ninput MomentInput { file: String!, widgets: [WidgetInput!] = [] }\n";
const MOMENT: &str = "operation :moment_create do\n  mutation \"MomentCreate\"\n  parameter :input, MomentInput\n  returns do\n    attribute :moment, MomentCreateResultMoment do\n      attribute :id, :id\n    end\n  end\nend\n";

#[test]
fn nested_input_lists_keep_every_dimension_and_nullability_in_both_languages() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("schema.graphql");
    fs::write(&path, "input MatrixInput { cells: [[Int!]!]! sparse: [[String]]! }").unwrap();
    let declared = parse_all(&["operation :send do\n  mutation \"Send\"\n  parameter :input, MatrixInput\nend\n"]).unwrap();
    let inputs = load_schema_types(Some(&path), &declared.scalars, &declared.models, &declared.operations).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &inputs);
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &inputs);
    assert!(swift.contains("public let cells: [[Int]]"), "{swift}");
    assert!(swift.contains("public let sparse: [[String?]?]"), "{swift}");
    assert!(kotlin.contains("val cells: List<List<Long>>"), "{kotlin}");
    assert!(kotlin.contains("val sparse: List<List<String?>?>"), "{kotlin}");
}

#[test]
fn a_json_scalar_in_an_input_object_is_raw_json_on_both_platforms_and_default_lists_stay_optional() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("schema.graphql");
    fs::write(&path, MOMENT_SCHEMA).unwrap();
    let declared = parse_all(&[MOMENT]).unwrap();
    let types = load_schema_types(Some(&path), &declared.scalars, &declared.models, &declared.operations).unwrap();
    let objects = &types.inputs;
    let settings = objects.iter().find(|object| object.name == "SettingsInput").unwrap();
    assert_eq!(settings.fields[0], field("itemProps", "json", false, false, false));
    assert_eq!(settings.fields[1], field("extras", "json", true, false, true));
    let widgets = objects.iter().find(|object| object.name == "MomentInput").unwrap().fields[1].clone();
    let mut expected = field("widgets", "WidgetInput", true, false, true);
    expected.ty.has_default = true;
    assert_eq!(widgets, expected);
    let swift = emit_swift(&declared.models, &declared.operations, &types);
    for line in ["    public let itemProps: JSONValue", "    public let extras: GraphQLNullable<[JSONValue]>", "    public let widgets: GraphQLNullable<[WidgetInput]>"] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &types);
    for line in ["val itemProps: JsonElement,", "val extras: GraphQLNullable<List<JsonElement>> = GraphQLNullable.Omitted"] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
}
