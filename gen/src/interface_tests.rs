use crate::{emit_kotlin, emit_swift, parse_models, SchemaTypes};
use dblayer::schema::FieldKind;

const SCREEN: &str = "model CustomScreen do
  field :slug, type: :string
  field :spec, type: ScreenSpec, null: true do
    field :root, type: :id
    field :elements, type: [ScreenElement] do
      shared do
        field :id, type: :id
        field :children, type: [:id]
        field :press, type: ScreenAction, null: true do
          on ScreenNavigateAction do
            field :slug, type: :string
          end
          on ScreenOpenChatAction do
            field :chatId, type: :string
          end
        end
      end
      on ScreenButton do
        field :label, type: :string, null: true
        field :size, type: :enum, values: [:small, :large], null: true
      end
      on ScreenHeading do
        field :text, type: :string
      end
      on ScreenPressable do
      end
    end
  end
end
";

fn names(fields: &[dblayer::schema::FieldSpec]) -> Vec<&str> {
    fields.iter().map(|field| field.name.as_str()).collect()
}

#[test]
fn a_list_of_interface_values_declares_the_shared_fields_once() {
    let models = parse_models(SCREEN).unwrap();
    let elements = &models[0].field("spec").unwrap().fields[1];
    assert_eq!((&elements.kind, elements.app_type.as_deref()), (&FieldKind::UnionArray, Some("ScreenElement")));
    assert_eq!(names(&elements.shared), ["id", "children", "press"]);
    assert_eq!(names(&elements.fields), ["button", "heading", "pressable"]);
    assert_eq!(names(&elements.fields[0].fields), ["label", "size"]);
    assert!(elements.fields[2].fields.is_empty());
}

#[test]
fn a_single_interface_value_and_a_list_reference_reuse_the_declaration() {
    let source = format!("{SCREEN}model Preview do\n  field :first, type: ScreenElement, null: true\n  field :all, type: [ScreenElement]\nend\n");
    let models = parse_models(&source).unwrap();
    let kinds: Vec<_> = models[1].fields.iter().skip(1).map(|field| (field.kind.clone(), field.app_type.clone())).collect();
    assert_eq!(kinds, [(FieldKind::Union, Some("ScreenElement".to_string())), (FieldKind::UnionArray, Some("ScreenElement".to_string()))]);
}

#[test]
fn interface_declarations_reject_invalid_shared_blocks_at_their_line() {
    let list = |body: &str| format!("model Sample do\n  field :v, type: [Value] do\n{body}  end\nend\n");
    let variant = "    on OneProps do\n      field :own, type: :string\n    end\n";
    let cases = [
        (list(&format!("    shared do\n      field :id, type: :id\n    end\n    shared do\n      field :other, type: :id\n    end\n{variant}")), "`shared` is declared twice"),
        (list(&format!("    shared do\n      field :own, type: :id\n    end\n{variant}")), "declared twice"),
        (list(&format!("    shared do\n      field :kind, type: :id\n    end\n{variant}")), "reserved"),
        (list(&format!("    shared do\n    end\n{variant}")), "at least 1 field"),
        (list("    on OneProps do\n    end\n"), "at least 1 field"),
        ("model Sample do\n  field :v, type: :object do\n    shared do\n      field :id, type: :id\n    end\n  end\nend\n".to_string(), "only inside a union"),
    ];
    for (source, error) in cases {
        let errors = parse_models(&source).expect_err(&source);
        assert!(errors.iter().any(|found| found.message.contains(error)), "{source}: {errors:?}");
    }
}

#[test]
fn swift_interfaces_are_enums_with_shared_properties_and_variant_structs_that_carry_the_shared_fields() {
    let swift = emit_swift(&parse_models(SCREEN).unwrap(), &[], &SchemaTypes::default());
    for token in [
        "public enum ScreenElement: Codable, Sendable, Equatable {",
        "case button(ScreenButton)",
        "case pressable(ScreenPressable)",
        "case unknown(typename: String)",
        "public var id: String? {",
        "case let .button(value): return value.id",
        "case .unknown: return nil",
        "public var children: [String]? {",
        "public var press: ScreenAction? {",
        "public struct ScreenButton: Codable, Sendable, Equatable {\n    public let id: String\n    public let children: [String]\n    public let press: ScreenAction?\n    public let label: String?",
        "public struct ScreenPressable: Codable, Sendable, Equatable {\n    public let id: String\n    public let children: [String]\n    public let press: ScreenAction?\n",
        "public let elements: [ScreenElement]",
        "try r.arr(1, ScreenElement.init(cells:))",
    ] {
        assert!(swift.contains(token), "missing Swift `{token}`:\n{swift}");
    }
}

#[test]
fn kotlin_interfaces_are_sealed_classes_with_shared_properties_and_variant_classes_that_carry_the_shared_fields() {
    let kotlin = emit_kotlin(&parse_models(SCREEN).unwrap(), &[], &SchemaTypes::default());
    for token in [
        "sealed class ScreenElement {",
        "data class Button(val value: ScreenButton) : ScreenElement()",
        "data class Unknown(val typename: String) : ScreenElement()",
        "val id: String?\n        get() = when (this) {\n            is Button -> value.id",
        "is Unknown -> null",
        "val press: ScreenAction?\n",
        "data class ScreenButton(\n    @Serializable(with = IDText::class) val id: String,\n    @Serializable(with = IDTexts::class) val children: List<String>,\n    val press: ScreenAction? = null,\n    val label: String? = null,",
        "val elements: List<ScreenElement>",
        "r.arr(1) { ScreenElement.fromCells(it) }",
    ] {
        assert!(kotlin.contains(token), "missing Kotlin `{token}`:\n{kotlin}");
    }
}

#[test]
fn a_list_of_variants_too_wide_for_one_sqlite_table_is_rejected_with_the_table_and_a_hint() {
    let variant = |at: usize| format!("    on Wide{at}Item do\n{}    end\n", (0..60).map(|member| format!("      field :m{member}, type: :string, null: true\n")).collect::<String>());
    let source = format!("model Screen do\n  field :spec, type: [ScreenItem] do\n    shared do\n      field :id, type: :id\n    end\n{}  end\nend\n", (0..40).map(variant).collect::<String>());
    let errors = parse_models(&source).unwrap_err();
    let expected = "model Screen: table row_Screen__spec would have 2406 columns, above the SQLite limit of 2000. A union stores its variants in the columns of one table: declare fewer variants or fields, or split the model";
    assert!(errors.iter().any(|found| found.message == expected), "{errors:?}");
}

#[test]
fn a_variant_reuses_a_type_declared_inside_the_shared_block() {
    let source = "model Sample do
  field :items, type: [Item] do
    shared do
      field :tag, type: Mark, null: true do
        on Gold do
          field :weight, type: :int
        end
        on Silver do
          field :purity, type: :num
        end
      end
    end
    on First do
      field :extra, type: Medal, null: true do
        on Gold
        on Silver
      end
    end
  end
end
";
    let models = parse_models(source).unwrap();
    let first = &models[0].field("items").unwrap().fields[0];
    assert_eq!(names(&first.fields), ["extra"]);
}
