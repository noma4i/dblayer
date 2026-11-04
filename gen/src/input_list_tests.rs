use std::fs;
use std::path::PathBuf;

use crate::{compile_files, emit_kotlin, emit_swift, Declarations};

const SCHEMA: &str = "type Query { a: Int }\ntype Mutation { widgetsSend(widgets: [WidgetInput!]): WidgetsSent, momentCreate(input: MomentCreateInput!): MomentCreated }\ntype WidgetsSent { id: ID! }\ntype MomentCreated { moment: Moment }\ntype Moment { id: ID! }\ninput MomentCreateInput { signedId: String!, widgets: [WidgetInput!] }\nenum WidgetKind { EMOJI GIF }\ninput TextInput { body: String! }\ninput EmojiInput { code: String!, kinds: [WidgetKind!] }\ninput PropsInput @oneOf { text: TextInput, emoji: EmojiInput }\ninput SettingsInput { kind: WidgetKind!, extras: [PropsInput!] }\ninput WidgetInput { kind: WidgetKind!, settings: SettingsInput, props: PropsInput! }\ndirective @oneOf on INPUT_OBJECT\n";

const UPLOAD_MUTATION: &str = "mutation MomentCreateFile($signedId: String!, $widgets: [WidgetInput!]) { momentCreate(input: { signedId: $signedId, widgets: $widgets }) { moment { id } } }\n";

fn operation(arg: &str) -> String {
    format!("operation :widgets_send do\n  mutation \"WidgetsSend\"\n  {}\n  returns do\n    attribute :id, :string\n  end\nend\n", crate::decl::parameter_line("widgets", arg))
}

fn upload(arg: &str) -> String {
    format!("model Moment do\n  attribute :text, :string, null: true\n  operation :create_file, upload: true do\n    mutation \"MomentCreateFile\"\n    optimistic false\n    {}\n    response from: \"momentCreate.moment\"\n  end\nend\n", crate::decl::parameter_line("widgets", arg))
}

fn compile(source: &str, mutation: &str, schema: &str) -> Result<Declarations, Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let schema_file = directory.path().join("schema.graphql");
    fs::write(&schema_file, schema).unwrap();
    fs::create_dir(directory.path().join("mutations")).unwrap();
    fs::write(directory.path().join("mutations/calls.graphql"), mutation).unwrap();
    let file = directory.path().join("sample.dbl");
    fs::write(&file, source).unwrap();
    let operations: Vec<PathBuf> = vec![directory.path().join("mutations")];
    compile_files(&[file], Some(&schema_file), &operations).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

const SEND_MUTATION: &str = "mutation WidgetsSend($widgets: [WidgetInput!]) { widgetsSend(widgets: $widgets) { id } }\n";

#[test]
fn an_operation_arg_takes_a_list_of_input_objects_in_every_nullability() {
    let cases = [
        ("[:WidgetInput]", "widgets: [WidgetInput]", "widgets: List<WidgetInput>", "variables_[\"widgets\"] = try JSONValue.encode(widgets)", "variables_[\"widgets\"] = jsonValue(widgets)"),
        ("[:WidgetInput]?", "widgets: [WidgetInput]? = nil", "widgets: List<WidgetInput>? = null", "if let value = widgets { variables_[\"widgets\"] = try JSONValue.encode(value) }", "if (widgets != null) variables_[\"widgets\"] = jsonValue(widgets)"),
        ("[:WidgetInput?]", "widgets: [WidgetInput?]", "widgets: List<WidgetInput?>", "variables_[\"widgets\"] = try JSONValue.encode(widgets)", "variables_[\"widgets\"] = jsonValue(widgets)"),
        ("[:WidgetInput?]?", "widgets: [WidgetInput?]? = nil", "widgets: List<WidgetInput?>? = null", "if let value = widgets { variables_[\"widgets\"] = try JSONValue.encode(value) }", "if (widgets != null) variables_[\"widgets\"] = jsonValue(widgets)"),
        ("[:WidgetKind]", "widgets: [WidgetKind]", "widgets: List<WidgetKind>", "variables_[\"widgets\"] = try JSONValue.encode(widgets)", "variables_[\"widgets\"] = jsonValue(widgets)"),
    ];
    for (arg, swift_param, kotlin_param, swift_put, kotlin_put) in cases {
        let schema = SCHEMA.replace("widgetsSend(widgets: [WidgetInput!]): WidgetsSent", if arg.contains("WidgetKind") { "widgetsSend(widgets: [WidgetKind!]): WidgetsSent" } else { "widgetsSend(widgets: [WidgetInput!]): WidgetsSent" });
        let mutation = if arg.contains("WidgetKind") { "mutation WidgetsSend($widgets: [WidgetKind!]) { widgetsSend(widgets: $widgets) { id } }\n" } else if arg.contains("WidgetInput?") { "mutation WidgetsSend($widgets: [WidgetInput]) { widgetsSend(widgets: $widgets) { id } }\n" } else { SEND_MUTATION };
        let declared = match compile(&operation(arg), mutation, &schema) {
            Ok(declared) => declared,
            Err(errors) => panic!("{arg}: {errors:?}"),
        };
        let swift = emit_swift(&declared.models, &declared.operations, &declared.inputs);
        let kotlin = emit_kotlin(&declared.models, &declared.operations, &declared.inputs);
        assert!(swift.contains(&format!("public static func widgetsSend({swift_param}, in scope_: AccountScope? = nil) async throws")), "{arg}\n{swift}");
        assert!(swift.contains(swift_put), "{arg}\n{swift}");
        assert!(kotlin.contains(&format!("suspend fun widgetsSend({kotlin_param}, account: AccountScope? = null)")), "{arg}\n{kotlin}");
        assert!(kotlin.contains(kotlin_put), "{arg}\n{kotlin}");
    }
}

#[test]
fn the_input_objects_behind_a_list_arg_are_generated_with_what_they_nest() {
    let declared = compile(&operation("[:WidgetInput]?"), SEND_MUTATION, SCHEMA).unwrap();
    let names: Vec<&str> = declared.inputs.inputs.iter().map(|input| input.name.as_str()).collect();
    assert_eq!(names, vec!["WidgetInput", "SettingsInput", "PropsInput", "TextInput", "EmojiInput"]);
    assert_eq!(declared.inputs.enums.iter().map(|declared| declared.name.as_str()).collect::<Vec<_>>(), vec!["WidgetKind"]);
}

#[test]
fn a_one_of_input_becomes_an_enum_with_one_case_per_field() {
    let declared = compile(&operation("[:WidgetInput]?"), SEND_MUTATION, SCHEMA).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &declared.inputs);
    for line in [
        "public indirect enum PropsInput: Encodable, Sendable, Equatable {\n    case text(TextInput)\n    case emoji(EmojiInput)\n",
        "        case .text(let value): try container.encode(value, forKey: .text)\n",
        "        case .emoji(let value): try container.encode(value, forKey: .emoji)\n",
        "    public let props: PropsInput\n",
        "    public let extras: GraphQLNullable<[PropsInput]>\n",
    ] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &declared.inputs);
    for line in [
        "@Serializable(with = PropsInputSerializer::class)\nsealed interface PropsInput {\n    @Immutable\n    data class Text(val value: TextInput) : PropsInput\n    @Immutable\n    data class Emoji(val value: EmojiInput) : PropsInput\n}\n",
        "object PropsInputSerializer : KSerializer<PropsInput> {",
        "is PropsInput.Text -> \"text\" to jsonValue(value.value)",
        "val props: PropsInput",
    ] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
}

#[test]
fn the_upload_args_of_a_model_become_a_typed_variables_builder_and_not_an_action_call() {
    let declared = compile(&upload("[:WidgetInput]?"), UPLOAD_MUTATION, SCHEMA).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &declared.inputs);
    assert!(swift.contains("public static func createFileVariables(widgets: [WidgetInput]? = nil) throws -> [String: JSONValue] {"), "{swift}");
    assert!(swift.contains("if let value = widgets { variables_[\"widgets\"] = try JSONValue.encode(value) }"), "{swift}");
    assert!(!swift.contains("func upload(") && !swift.contains("act(\"upload\""), "{swift}");
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &declared.inputs);
    assert!(kotlin.contains("fun createFileVariables(widgets: List<WidgetInput>? = null): JsonObject = buildJsonObject {"), "{kotlin}");
    assert!(kotlin.contains("if (widgets != null) this.put(\"widgets\", jsonValue(widgets))"), "{kotlin}");
    assert!(!kotlin.contains("suspend fun upload(") && !kotlin.contains("act(\"upload\""), "{kotlin}");
}

#[test]
fn an_upload_without_args_needs_neither_the_signed_id_nor_a_builder() {
    let source = "model Moment do\n  attribute :text, :string, null: true\n  operation :create_file, upload: true do\n    mutation \"MomentCreateFile\"\n    optimistic false\n    response from: \"momentCreate.moment\"\n  end\nend\n";
    let declared = compile(source, UPLOAD_MUTATION, SCHEMA).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &declared.inputs);
    assert!(!swift.contains("createFileVariables") && !swift.contains("act(\"upload\""), "{swift}");
}

#[test]
fn a_list_arg_is_checked_against_the_variable_of_the_mutation() {
    let cases = [
        ("[:WidgetInput]", None),
        ("[:WidgetInput]?", None),
        ("[:WidgetInput?]", Some("parameter `widgets` of kind `[WidgetInput?]` does not fit `widgets: [WidgetInput!]`")),
        ("[:WidgetInput?]?", Some("parameter `widgets` of kind `[WidgetInput?]` does not fit `widgets: [WidgetInput!]`")),
        (":WidgetInput", Some("parameter `widgets` of kind `WidgetInput` does not fit `widgets: [WidgetInput!]`")),
        ("[:SettingsInput]?", Some("parameter `widgets` of kind `[SettingsInput]` does not fit `widgets: [WidgetInput!]`")),
    ];
    for (arg, expected) in cases {
        let result = compile(&upload(arg), UPLOAD_MUTATION, SCHEMA);
        match (result, expected) {
            (Ok(_), None) => {}
            (Err(errors), Some(message)) => assert!(errors.iter().any(|error| error.contains(message)), "{arg}: {errors:?}"),
            (other, expected) => panic!("{arg}: expected {expected:?}, got {:?}", other.map(|_| ())),
        }
    }
}

#[test]
fn an_upload_arg_the_mutation_does_not_declare_is_named() {
    let errors = compile(&upload("[:WidgetInput]?").replace("parameter :widgets", "parameter :pictures"), UPLOAD_MUTATION, SCHEMA).unwrap_err();
    assert!(errors.iter().any(|error| error.contains("`pictures` is not an input of operation `MomentCreateFile`")), "{errors:?}");
}

#[test]
fn a_list_of_scalars_is_not_an_arg_kind() {
    let errors = compile(&operation("[:string]"), SEND_MUTATION, SCHEMA).unwrap_err();
    assert!(errors.iter().any(|error| error.contains(crate::decl::PARAM_TYPES)), "{errors:?}");
}

#[test]
fn list_args_of_create_and_of_other_model_actions_become_typed_parameters() {
    let source = "model Moment do\n  attribute :text, :string, null: true\n  operation :create, durable: true do\n    mutation \"WidgetsSend\"\n    parameter :widgets, [WidgetInput], null: true\n    variables widgets: :widgets\n  end\n  operation :decorate do\n    mutation \"WidgetsSend\"\n    optimistic false\n    parameter :widgets, [WidgetInput]\n    variables widgets: :widgets\n  end\nend\n";
    let declared = compile(source, SEND_MUTATION, SCHEMA).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &declared.inputs);
    for line in [
        "widgets: [WidgetInput]? = nil",
        "@concurrent public func decorate(widgets: [WidgetInput], in scope_: AccountScope? = nil) async throws -> Self {",
        "\"widgets\": try JSONValue.encode(widgets)",
    ] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &declared.inputs);
    for line in [
        "widgets: List<WidgetInput>? = null",
        "suspend fun decorate(widgets: List<WidgetInput>, account: AccountScope? = null): Moment = this.act(\"decorate\"",
    ] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
}
