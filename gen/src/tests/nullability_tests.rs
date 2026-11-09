use super::*;

const SOURCE: &str = r#"model Doc do
  attribute :top, DocTop do
    attribute :a, :string
  end
  attribute :topNull, DocTopNull, null: true do
    attribute :a, :string
  end
  attribute :versions, [DocVersion], default: [] do
    attribute :requires, DocRequires do
      attribute :min, :string
    end
    attribute :requiresNull, DocRequiresNull, null: true do
      attribute :min, :string
    end
    attribute :requiresOpt, DocRequiresOpt, optional: true do
      attribute :min, :string
    end
    attribute :rows, [DocRow], default: [] do
      attribute :cell, DocCell do
        attribute :v, :string
      end
    end
  end
  attribute :props, DocProps do
    on DocPropsA do
      attribute :inner, DocInner do
        attribute :x, :string
      end
      attribute :innerNull, DocInnerNull, null: true do
        attribute :x, :string
      end
    end
  end
end
"#;

#[test]
fn gen_nested_nullability_every_depth() {
    let models = parse_models(SOURCE).unwrap();
    let swift = emit_swift(&models, &[], &Default::default());
    let kotlin = emit_kotlin(&models, &[], &Default::default());
    let rows: [(&str, &str, &str, &str, &str); 8] = [
        ("row attribute", "    public let top: DocTop\n", "    val top: DocTop,\n", "r.obj(0, DocTop.init(cells:))", "r.obj(0) { DocTop.fromCells(it) }"),
        ("row attribute null", "    public let topNull: DocTopNull?\n", "    val topNull: DocTopNull? = null,\n", "r.objOpt(1, DocTopNull.init(cells:))", "r.objOpt(1) { DocTopNull.fromCells(it) }"),
        ("list item field", "    public let requires: DocRequires\n", "    val requires: DocRequires,\n", "r.obj(0, DocRequires.init(cells:))", "r.obj(0) { DocRequires.fromCells(it) }"),
        ("list item field null", "    public let requiresNull: DocRequiresNull?\n", "    val requiresNull: DocRequiresNull? = null,\n", "r.objOpt(1, DocRequiresNull.init(cells:))", "r.objOpt(1) { DocRequiresNull.fromCells(it) }"),
        ("list item field optional", "    public let requiresOpt: DocRequiresOpt?\n", "    val requiresOpt: DocRequiresOpt? = null,\n", "r.objOpt(2, DocRequiresOpt.init(cells:))", "r.objOpt(2) { DocRequiresOpt.fromCells(it) }"),
        ("array in array", "    public let cell: DocCell\n", "    val cell: DocCell,\n", "r.obj(0, DocCell.init(cells:))", "r.obj(0) { DocCell.fromCells(it) }"),
        ("union variant field", "    public let inner: DocInner\n", "    val inner: DocInner,\n", "r.obj(0, DocInner.init(cells:))", "r.obj(0) { DocInner.fromCells(it) }"),
        ("union variant field null", "    public let innerNull: DocInnerNull?\n", "    val innerNull: DocInnerNull? = null,\n", "r.objOpt(1, DocInnerNull.init(cells:))", "r.objOpt(1) { DocInnerNull.fromCells(it) }"),
    ];
    for (row, swift_type, kotlin_type, swift_read, kotlin_read) in rows {
        assert!(swift.contains(swift_type), "{row}: Swift type {swift_type}");
        assert!(kotlin.contains(kotlin_type), "{row}: Kotlin type {kotlin_type}");
        assert!(swift.contains(swift_read), "{row}: Swift decoder {swift_read}");
        assert!(kotlin.contains(kotlin_read), "{row}: Kotlin decoder {kotlin_read}");
    }
}

#[test]
fn gen_nested_nullability_ignores_same_named_root_schema_field() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("doc.dbl");
    let schema = directory.path().join("schema.graphql");
    let queries = directory.path().join("queries");
    fs::write(&file, "model Doc do\n  attribute :requires, DocRequires, null: true do\n    attribute :ios, :int, null: true\n  end\n  attribute :versions, [DocVersion], default: [] do\n    attribute :requires, DocVersionRequires do\n      attribute :ios, :int, null: true\n    end\n  end\n  scope :all do\n    query \"Docs\"\n    response shape: :list\n  end\nend\n").unwrap();
    fs::write(&schema, "type Query { docs: [Doc!]! }\ntype Doc { id: ID! requires: Requires versions: [Version!]! }\ntype Version { requires: Requires! }\ntype Requires { ios: Int }").unwrap();
    fs::create_dir(&queries).unwrap();
    fs::write(queries.join("docs.graphql"), "query Docs { docs { id requires { ios } versions { requires { ios } } } }").unwrap();
    let declared = compile_files(&[file], Some(&schema), &[queries]).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &Default::default());
    assert!(swift.contains("    public let requires: DocRequires?\n") && swift.contains("    public let requires: DocVersionRequires\n"), "{swift}");
}
