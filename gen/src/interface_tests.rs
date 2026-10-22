use crate::{emit_kotlin, emit_swift, parse_models, SchemaTypes};
use dblayer::schema::FieldKind;

const SAMPLE: &str = "model InterfaceRecord do
  field :slug, type: :string
  field :spec, type: SampleSpec, null: true do
    field :root, type: :id
    field :elements, type: [SampleElement] do
      shared do
        field :id, type: :id
        field :children, type: [:id]
        field :press, type: SampleAction, null: true do
          on SampleNavigateAction do
            field :slug, type: :string
          end
          on SampleOpenRecordAction do
            field :recordId, type: :string
          end
        end
      end
      on SampleButton do
        field :label, type: :string, null: true
        field :size, type: :enum, values: [:small, :large], null: true
      end
      on SampleHeading do
        field :text, type: :string
      end
      on SamplePressable do
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
    let models = parse_models(SAMPLE).unwrap();
    let elements = &models[0].field("spec").unwrap().fields[1];
    assert_eq!((&elements.kind, elements.app_type.as_deref()), (&FieldKind::UnionArray, Some("SampleElement")));
    assert_eq!(names(&elements.shared), ["id", "children", "press"]);
    assert_eq!(names(&elements.fields), ["button", "heading", "pressable"]);
    assert_eq!(names(&elements.fields[0].fields), ["label", "size"]);
    assert!(elements.fields[2].fields.is_empty());
}

#[test]
fn a_single_interface_value_and_a_list_reference_reuse_the_declaration() {
    let source = format!("{SAMPLE}model Preview do\n  field :first, type: SampleElement, null: true\n  field :all, type: [SampleElement]\nend\n");
    let models = parse_models(&source).unwrap();
    let kinds: Vec<_> = models[1].fields.iter().skip(1).map(|field| (field.kind.clone(), field.app_type.clone())).collect();
    assert_eq!(kinds, [(FieldKind::Union, Some("SampleElement".to_string())), (FieldKind::UnionArray, Some("SampleElement".to_string()))]);
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
    let swift = emit_swift(&parse_models(SAMPLE).unwrap(), &[], &SchemaTypes::default());
    for token in [
        "public enum SampleElement: Codable, Sendable, Equatable {",
        "case button(SampleButton)",
        "case pressable(SamplePressable)",
        "case unknown(typename: String)",
        "public var id: String? {",
        "case let .button(value): return value.id",
        "case .unknown: return nil",
        "public var children: [String]? {",
        "public var press: SampleAction? {",
        "public struct SampleButton: Codable, Sendable, Equatable {\n    public let id: String\n    public let children: [String]\n    public let press: SampleAction?\n    public let label: String?",
        "public struct SamplePressable: Codable, Sendable, Equatable {\n    public let id: String\n    public let children: [String]\n    public let press: SampleAction?\n",
        "public let elements: [SampleElement]",
        "try r.arr(1, SampleElement.init(cells:))",
    ] {
        assert!(swift.contains(token), "missing Swift `{token}`:\n{swift}");
    }
}

#[test]
fn kotlin_interfaces_are_sealed_classes_with_shared_properties_and_variant_classes_that_carry_the_shared_fields() {
    let kotlin = emit_kotlin(&parse_models(SAMPLE).unwrap(), &[], &SchemaTypes::default());
    for token in [
        "sealed class SampleElement {",
        "data class Button(val value: SampleButton) : SampleElement()",
        "data class Unknown(val typename: String) : SampleElement()",
        "val id: String?\n        get() = when (this) {\n            is Button -> value.id",
        "is Unknown -> null",
        "val press: SampleAction?\n",
        "data class SampleButton(\n    @Serializable(with = IDText::class) val id: String,\n    @Serializable(with = IDTexts::class) val children: List<String>,\n    val press: SampleAction? = null,\n    val label: String? = null,",
        "val elements: List<SampleElement>",
        "r.arr(1) { SampleElement.fromCells(it) }",
    ] {
        assert!(kotlin.contains(token), "missing Kotlin `{token}`:\n{kotlin}");
    }
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

#[test]
fn catalog_widths_fit_until_expanded_and_raw_elements_keep_a_constant_width() {
    // Fixed-shape catalogue example: 102 components and 20 base entries.
    let widths = [
        (0,2,0),(0,3,0),(0,1,0),(1,1,0),(0,1,0),(0,2,0),(2,3,0),(0,3,0),(0,2,0),(1,1,0),
        (3,1,0),(0,0,1),(4,2,0),(2,0,0),(1,3,0),(0,2,0),(1,3,0),(1,3,0),(1,3,0),(0,0,0),
        (1,1,0),(3,0,0),(1,0,0),(2,0,0),(1,0,0),(1,1,0),(0,0,0),(1,2,1),(1,1,0),(0,0,0),
        (5,2,0),(2,2,0),(3,1,0),(2,0,0),(1,0,0),(1,0,0),(1,2,0),(2,1,0),(2,1,0),(0,1,0),
        (3,0,0),(2,0,0),(1,0,0),(2,0,0),(2,2,0),(0,0,0),(1,3,1),(3,0,0),(2,1,0),(0,3,0),
        (0,0,1),(1,1,1),(3,0,0),(2,0,0),(1,0,0),(1,0,0),(1,1,0),(2,0,0),(1,4,0),(3,0,0),
        (6,0,0),(1,0,0),(1,0,0),(1,0,0),(1,0,0),(0,2,0),(1,1,0),(1,2,0),(1,0,0),(1,0,0),
        (0,1,1),(2,0,0),(0,0,1),(3,1,0),(0,1,0),(0,0,1),(1,0,0),(0,0,0),(2,0,1),(5,0,0),
        (3,3,0),(3,0,0),(0,1,0),(0,1,0),(1,0,0),(1,2,0),(3,1,0),(0,1,0),(1,0,0),(1,0,0),
        (0,0,1),(3,0,0),(5,0,0),(2,0,0),(0,2,0),(1,0,0),(1,1,0),(2,0,0),(1,0,0),(2,1,0),
        (1,1,0),(0,2,0),(1,0,0),(0,1,0),(1,0,0),(1,2,0),(4,0,0),(0,2,0),(2,2,0),(1,0,1),
        (0,0,0),(3,3,0),(2,0,0),(1,1,0),(0,0,0),(1,0,0),(1,1,0),(1,1,0),(4,2,0),(2,0,0),
        (2,3,0),(1,0,0),
    ];
    let total = |values: &[(usize, usize, usize)]| values.iter().map(|(v,s,_)| 5*v+s).sum::<usize>();
    assert_eq!((widths.len()-20, total(&widths[20..]), total(&widths)), (102,807,928));
    let variants = |copies: usize| widths.iter().cycle().take(widths.len()*copies).enumerate().map(|(i,(v,s,l))| {
        let values = (0..*v).map(|j| format!("field :v{j}, type: Value{i}_{j} do\non Text{i}_{j} do\nfield :v, type: :string\nend\non Number{i}_{j} do\nfield :v, type: :num\nend\non Flag{i}_{j} do\nfield :v, type: :bool\nend\non Ref{i}_{j} do\nfield :v, type: :string\nend\nend\n")).collect::<String>();
        let scalars = (0..*s).map(|j| format!("field :s{j}, type: :string\n")).collect::<String>();
        let lists = (0..*l).map(|j| format!("field :l{j}, type: [:string]\n")).collect::<String>();
        format!("on Item{i} do\n{values}{scalars}{lists}end\n")
    }).collect::<String>();
    let shared = (0..13).map(|i| format!("field :common{i}, type: :string\n")).collect::<String>();
    let source = |copies| format!("model Sample do\nfield :items, type: [Item] do\nshared do\n{shared}end\n{}end\nend\n", variants(copies));
    let models = parse_models(&source(1)).unwrap();
    assert_eq!(dblayer::ddl::table_widths(&models[0]).into_iter().find(|(name,_)| name == "row_Sample__items").unwrap().1, 957);
    let errors = parse_models(&source(3)).unwrap_err();
    let expected = "model Sample: table row_Sample__items would have 2835 columns, above the SQLite limit of 2000. A union stores its variants in the columns of one table: declare fewer variants or fields, or split the model";
    assert!(errors.iter().any(|e| e.message == expected), "{errors:?}");
    let models = parse_models(include_str!("../../fixtures/screen.dbl")).unwrap();
    let elements = models[0].field("spec").unwrap().fields.iter().find(|f| f.name == "elements").unwrap();
    assert_eq!(elements.kind, FieldKind::Array);
    assert_eq!(elements.fields.iter().find(|f| f.name == "props").unwrap().kind, FieldKind::Raw);
    assert!(elements.fields.iter().find(|f| f.name == "props").unwrap().parse_json);
    assert_eq!(dblayer::ddl::table_widths(&models[0]).into_iter().find(|(name,_)| name == "row_CustomScreen__spec__elements").unwrap().1, 19);
    assert_eq!(dblayer::ddl::table_widths(&models[0]).into_iter().find(|(name,_)| name == "row_CustomScreen__spec__state").unwrap().1, 6);
}
