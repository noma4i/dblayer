use crate::members::{redeclared, Platform};
use crate::{emit_kotlin, emit_swift, parse_files, SchemaTypes};
use std::path::PathBuf;

fn repeated(source: &str, platform: Platform) -> Vec<String> {
    redeclared(source, platform).into_iter().map(|again| format!("{} {} in {}", again.kind, again.shown, again.path)).collect()
}

#[test]
fn the_member_parser_flags_only_declarations_that_repeat_in_one_type() {
    let swift = "public struct Page: DBModel {\n    public static let modelName = \"Page\"\n    public let modelName: String\n    public static var modelName: Query<Self> {\n        Query<Self>()\n    }\n    public static func mark(_ note: String) throws {}\n    public static func mark(_ note: Int) throws {}\n    public static func mark(_ other: String) throws {}\n    public func mark(_ note: String) {}\n    public init(id: String) {}\n    public init(wire row: WireRow) throws {}\n}\n\nextension Page {\n    public static let modelName = \"Again\"\n}\n";
    assert_eq!(
        repeated(swift, Platform::Swift),
        ["static property modelName in Page", "static function mark(_:String) in Page", "static property modelName in Page"],
        "Swift rows: instance and static differ, labels and types make overloads, the same labels and types repeat, extensions share the type"
    );
    let kotlin = "data class Page(\n    override val id: String,\n    val title: String = \"\",\n) : DBModel {\n    override val modelName get() = \"Page\"\n    companion object : ModelDecoder<Page> {\n        override val modelName = \"Page\"\n        val modelName: Query<Page> get() = Query(\"Page\", this)\n        fun mark(note: String) = 1\n        fun mark(other: String) = 2\n        fun mark(note: Int) = 3\n        val mark = 4\n    }\n    fun title(): String = \"\"\n    val title: Int = 1\n}\n";
    let extensions = "val BlobUpload.Companion.VideoIntro: BlobUpload get() = BlobUpload(\"a\", \"B\")\nval BlobUpload.Companion.VideoIntro: BlobUpload get() = BlobUpload(\"b\", \"B\")\nval BlobUpload.Companion.Other: BlobUpload get() = BlobUpload(\"c\", \"B\")\n";
    assert_eq!(repeated(extensions, Platform::Kotlin), ["property VideoIntro in BlobUpload.Companion"], "Kotlin extension properties live under their receiver");
    assert_eq!(
        repeated(kotlin, Platform::Kotlin),
        ["property modelName in Page > companion", "function mark(String) in Page > companion", "property title in Page"],
        "Kotlin rows: class and companion differ, parameter names do not make an overload, a function and a property differ"
    );
}

#[test]
fn the_generated_output_of_every_fixture_declares_each_member_once() {
    let directory = crate::package_dir().join("../fixtures");
    let mut paths: Vec<PathBuf> = std::fs::read_dir(directory).unwrap().map(|entry| entry.unwrap().path()).filter(|path| path.extension().is_some_and(|extension| extension == "dbl")).collect();
    paths.sort();
    let models = parse_files(&paths).unwrap();
    let swift = emit_swift(&models, &[], &SchemaTypes::default());
    let kotlin = emit_kotlin(&models, &[], &SchemaTypes::default());
    for (platform, source) in [(Platform::Swift, swift), (Platform::Kotlin, kotlin)] {
        assert_eq!(repeated(&source, platform), Vec::<String>::new(), "{}", platform.label());
        let planted = source.replacen("public static let modelName = \"Ledger\"", "public static let modelName = \"Ledger\"\n    public static let modelName = \"Ledger\"", 1).replacen("override val modelName = \"Ledger\"", "override val modelName = \"Ledger\"\n        override val modelName = \"Ledger\"", 1);
        assert_eq!(repeated(&planted, platform).len(), 1, "{}: a planted repeat must be found, so the parser sees the members of the real output", platform.label());
    }
}
