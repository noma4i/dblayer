use std::fs;
use std::path::PathBuf;

use crate::{compile_files, emit_kotlin, emit_swift, parse_all, parse_models, EnumDecl};

fn decl(values: &[&str]) -> EnumDecl {
    EnumDecl::new("Sample", &values.iter().map(|value| value.to_string()).collect::<Vec<_>>())
}

#[test]
fn case_names_follow_each_language_and_bad_values_are_rejected() {
    let values = ["like", "HAPPY", "FOO_BAR", "fooBar2", "text/plain", "default"];
    let declared = decl(&values);
    assert_eq!(declared.cases(true), ["like", "happy", "fooBar", "fooBar2", "textPlain", "`default`"]);
    assert_eq!(declared.cases(false), ["Like", "Happy", "FooBar", "FooBar2", "TextPlain", "Default"]);
    for (case, values, error) in [
        ("collision", vec!["a_b", "aB"], "both become the Swift case `aB`"),
        ("digit", vec!["1st"], "cannot become a Swift case name"),
        ("empty", vec!["__"], "cannot become a Swift case name"),
        ("reserved Swift", vec!["unknown"], "reserved Swift case `unknown`"),
        ("reserved Kotlin", vec!["Companion"], "reserved"),
        ("no values", vec![], "has no values"),
    ] {
        let message = decl(&values).check().expect_err(case);
        assert!(message.contains(error), "{case}: {message}");
    }
}

const VIBE: &str = "model Vibe do\n\n  field :kind, type: :enum, values: [:like, :love]\n  field :mood, type: :enum, values: [:calm], null: true, as: Mood\n  field :meta, type: Meta, null: true do\n    field :state, type: :enum, values: [:ready, :done], default: :ready\n  end\nend\n";

#[test]
fn model_and_nested_enums_are_typed_enums_with_an_unknown_case_on_both_platforms() {
    let models = parse_models(VIBE).unwrap();
    let swift = emit_swift(&models, &[], &Default::default());
    for line in [
        "public enum VibeKind: Codable, Sendable, Hashable {\n    case like\n    case love\n    case unknown(String)\n",
        "        case \"like\": self = .like\n",
        "        default: self = .unknown(rawValue)\n",
        "        case .unknown(let raw): return raw\n",
        "public enum Mood: Codable, Sendable, Hashable {",
        "public enum MetaState: Codable, Sendable, Hashable {",
        "    public let kind: VibeKind\n",
        "    public let mood: Mood?\n",
        "    public static let kind = Column<Vibe, VibeKind>(\"kind\")",
        "kind = try VibeKind(rawValue: r.req(",
        "mood = try r.opt(",
        ").map(Mood.init(rawValue:))",
        "state: MetaState = MetaState.ready",
        "?? MetaState.ready",
    ] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&models, &[], &Default::default());
    for line in [
        "@Serializable(with = VibeKindSerializer::class)\nsealed class VibeKind(override val raw: String) : RawEnum {\n    data object Like : VibeKind(\"like\")\n",
        "    data class Unknown(val value: String) : VibeKind(value)\n",
        "            else -> Unknown(raw)\n",
        "object VibeKindSerializer : RawEnumSerializer<VibeKind>(\"VibeKind\", { VibeKind.of(it) })",
        "    val kind: VibeKind,\n",
        "    val mood: Mood? = null,\n",
        "val kind = Column<Vibe, VibeKind>(\"kind\")",
        ".let { VibeKind.of(it) }",
        "val state: MetaState = MetaState.Ready,",
    ] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
    assert!(!swift.contains("kind: String") && !kotlin.contains("kind: String"), "an enum field is not a String any more");
}

#[test]
fn one_enum_name_with_different_values_and_bad_values_fail_at_their_line() {
    for (fields, error) in [
        ("field :a, type: :enum, values: [:x], as: Same\nfield :b, type: :enum, values: [:y], as: Same", "different values"),
        ("field :a, type: :enum, values: [:a_b, :aB]", "both become"),
        ("field :a, type: :enum, values: [:x], as: Sample", "name of a model"),
    ] {
        let source = format!("model Sample do\n\n{fields}\n\nend");
        let errors = parse_models(&source).expect_err(&source);
        assert!(errors.iter().any(|found| found.line >= 3 && found.message.contains(error)), "{source}: {errors:?}");
    }
}

pub(crate) const SCHEMA: &str = "type Query { a: Int }\nscalar Cursor\nscalar Wide\nscalar Mystery\nenum WidgetKind { EMOJI GIF }\ninput WidgetInput { kind: WidgetKind!, after: Cursor, size: Wide! }\ntype Mutation { widgetSend(input: WidgetInput!, kind: WidgetKind!): WidgetSent }\ntype WidgetSent { kind: WidgetKind! size: Wide! }\n";
const OPERATIONS: &str = "mutation WidgetSend($input: WidgetInput!, $kind: WidgetKind!) { widgetSend(input: $input, kind: $kind) { kind size } }\n";
pub(crate) const SCALARS: &str = "scalar :Cursor, as: :string\nscalar :Wide, as: :long\n";
pub(crate) const SEND: &str = "operation :widget_send do\n  mutation WidgetSend\n  args input: :WidgetInput, kind: :WidgetKind\n  result kind: :WidgetKind, size: :int\nend\n";

pub(crate) fn compile(files: &[&str], schema: &str) -> Result<crate::Declarations, Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let schema_path = directory.path().join("schema.graphql");
    fs::write(&schema_path, schema).unwrap();
    fs::create_dir(directory.path().join("queries")).unwrap();
    fs::write(directory.path().join("queries/widget.graphql"), OPERATIONS).unwrap();
    let paths: Vec<PathBuf> = files.iter().enumerate().map(|(at, source)| {
        let path = directory.path().join(format!("{at}.dbl"));
        fs::write(&path, source).unwrap();
        path
    }).collect();
    compile_files(&paths, Some(&schema_path), &[directory.path().join("queries")]).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

#[test]
fn schema_enums_become_typed_enums_in_inputs_args_and_results_and_scalars_follow_their_mapping() {
    let declared = compile(&[SCALARS, SEND], SCHEMA).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &declared.inputs);
    for line in [
        "public enum WidgetKind: Codable, Sendable, Hashable {\n    case emoji\n    case gif\n",
        "        case \"EMOJI\": self = .emoji\n",
        "    public let kind: WidgetKind\n",
        "    public let after: GraphQLNullable<String>\n",
        "    public let size: Int\n",
        "public static func widgetSend(input: WidgetInput, kind: WidgetKind) async throws -> WidgetSendResult {",
        "    public let kind: WidgetKind?\n",
    ] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    assert_eq!(swift.matches("public enum WidgetKind").count(), 1, "one declaration per enum");
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &declared.inputs);
    for line in [
        "sealed class WidgetKind(override val raw: String) : RawEnum {\n    data object Emoji : WidgetKind(\"EMOJI\")\n",
        "val kind: WidgetKind,",
        "val size: Long",
        "suspend fun widgetSend(input: WidgetInput, kind: WidgetKind): WidgetSendResult {",
        "val kind: WidgetKind? = null",
    ] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
}

#[test]
fn an_unmapped_custom_scalar_a_string_arg_over_an_enum_and_a_result_enum_without_schema_fail_with_a_reason() {
    let unmapped = compile(&[SEND], SCHEMA).unwrap_err();
    assert!(unmapped.iter().any(|message| message.contains("custom scalar `Cursor` has no mapping: add `scalar :Cursor, as: :string`")), "{unmapped:?}");
    let string_arg = compile(&[SCALARS, &SEND.replace("kind: :WidgetKind\n", "kind: :string\n")], SCHEMA).unwrap_err();
    assert!(string_arg.iter().any(|message| message.contains("does not fit `WidgetKind!`")), "{string_arg:?}");
    let errors = parse_all(&["scalar :Cursor, as: :bogus\n"]).err().unwrap();
    assert!(errors[0].1.message.contains("scalar kind must be one of"), "{errors:?}");
    let declared = parse_all(&[SEND]).unwrap();
    assert_eq!(crate::load_schema_types(None, &declared.scalars, &declared.models, &declared.operations).unwrap_err().contains("--schema"), true);
}
