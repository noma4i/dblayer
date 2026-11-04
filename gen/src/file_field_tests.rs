use crate::SchemaTypes;
use crate::{emit_kotlin, emit_swift, parse_models};

#[test]
fn a_file_field_is_read_as_a_description_and_written_only_through_attach() {
    let models = parse_models("model Sample do\n\n    attribute :name, :string\n    attribute :media, :file, null: true\nend\n").unwrap();
    let swift = emit_swift(&models, &[], &SchemaTypes::default());
    for expected in ["public let media: FileInfo?", "public enum Files: String, Sendable { case media }", "try r.fileOpt(", "create(name: String, in scope_: AccountScope? = nil)", "update(name: String? = nil, in scope_: AccountScope? = nil)"] {
        assert!(swift.contains(expected), "swift lacks {expected}");
    }
    assert!(!swift.contains("Column<Sample, FileInfo"), "a file field has no column");
    let kotlin = emit_kotlin(&models, &[], &SchemaTypes::default());
    for expected in ["val media: FileInfo? = null", "val media = FileField<Sample>(\"media\")", "r.fileOpt(", "suspend fun create(name: String, account: AccountScope? = null): MutationReceipt<Sample>"] {
        assert!(kotlin.contains(expected), "kotlin lacks {expected}");
    }
    assert!(!kotlin.contains("Column<Sample, FileInfo"), "a file field has no column");
}
