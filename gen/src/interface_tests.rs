use crate::{emit_kotlin, emit_swift, parse_models, SchemaTypes};
use dblayer::schema::FieldKind;

const SAMPLE: &str = "model InterfaceRecord do
  attribute :slug, :string
  attribute :spec, SampleSpec, null: true do
    attribute :root, :id
    attribute :elements, [SampleElement] do
      shared do
        attribute :id, :id
        attribute :children, [:id]
        attribute :press, SampleAction, null: true do
          on SampleNavigateAction do
            attribute :slug, :string
          end
          on SampleOpenRecordAction do
            attribute :recordId, :string
          end
        end
      end
      on SampleButton do
        attribute :label, :string, null: true
        attribute :size, :enum, values: [\"small\", \"large\"], null: true
      end
      on SampleHeading do
        attribute :text, :string
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
    let source = format!("{SAMPLE}model Preview do\n  attribute :first, SampleElement, null: true\n  attribute :all, [SampleElement]\nend\n");
    let models = parse_models(&source).unwrap();
    let kinds: Vec<_> = models[1].fields.iter().skip(1).map(|field| (field.kind.clone(), field.app_type.clone())).collect();
    assert_eq!(kinds, [(FieldKind::Union, Some("SampleElement".to_string())), (FieldKind::UnionArray, Some("SampleElement".to_string()))]);
}

#[test]
fn interface_declarations_reject_invalid_shared_blocks_at_their_line() {
    let list = |body: &str| format!("model Sample do\n  attribute :v, [Value] do\n{body}  end\nend\n");
    let variant = "    on OneProps do\n      attribute :own, :string\n    end\n";
    let cases = [
        (list(&format!("    shared do\n      attribute :id, :id\n    end\n    shared do\n      attribute :other, :id\n    end\n{variant}")), "`shared` is declared twice"),
        (list(&format!("    shared do\n      attribute :own, :id\n    end\n{variant}")), "declared twice"),
        (list(&format!("    shared do\n      attribute :kind, :id\n    end\n{variant}")), "reserved"),
        (list(&format!("    shared do\n    end\n{variant}")), "at least 1 attribute"),
        (list("    on OneProps do\n    end\n"), "at least 1 attribute"),
        ("model Sample do\n  attribute :v, SampleV do\n    shared do\n      attribute :id, :id\n    end\n  end\nend\n".to_string(), "has no variants"),
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
  attribute :items, [Item] do
    shared do
      attribute :tag, Mark, null: true do
        on Gold do
          attribute :weight, :int
        end
        on Silver do
          attribute :purity, :num
        end
      end
    end
    on First do
      attribute :extra, Medal, null: true do
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
        let values = (0..*v).map(|j| format!("attribute :v{j}, Value{i}_{j} do\non Text{i}_{j} do\nattribute :v, :string\nend\non Number{i}_{j} do\nattribute :v, :num\nend\non Flag{i}_{j} do\nattribute :v, :bool\nend\non Ref{i}_{j} do\nattribute :v, :string\nend\nend\n")).collect::<String>();
        let scalars = (0..*s).map(|j| format!("attribute :s{j}, :string\n")).collect::<String>();
        let lists = (0..*l).map(|j| format!("attribute :l{j}, [:string]\n")).collect::<String>();
        format!("on Item{i} do\n{values}{scalars}{lists}end\n")
    }).collect::<String>();
    let shared = (0..13).map(|i| format!("attribute :common{i}, :string\n")).collect::<String>();
    let source = |copies| format!("model Sample do\nattribute :items, [Item] do\nshared do\n{shared}end\n{}end\nend\n", variants(copies));
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
