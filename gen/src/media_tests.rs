use std::fs;
use crate::{compile_files, emit_kotlin, emit_swift};

#[test]
fn media_marker_platforms() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().join("fixtures/operations");
    let source = fs::read_to_string(root.join("media.dbl")).unwrap();
    let schema = fs::read_to_string(root.join("schema.graphql")).unwrap();
    let operations = fs::read_to_string(root.join("queries/media.graphql")).unwrap();
    let compile = |source: &str, schema: &str, operations: &str| {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("media.dbl");
        let schema_file = directory.path().join("schema.graphql");
        let query = directory.path().join("media.graphql");
        fs::write(&file, source).unwrap();
        fs::write(&schema_file, schema).unwrap();
        fs::write(&query, operations).unwrap();
        compile_files(&[file], Some(&schema_file), &[query])
    };
    let declared = compile(&source, &schema, &operations).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &declared.inputs);
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &declared.inputs);
    assert!(swift.contains("func media(_ chatId: String) throws -> Query<Self>"), "{swift}");
    assert!(kotlin.contains("fun media(chatId: String): Query<MediaEntry>"), "{kotlin}");
    let media = declared.models[0].relations[0].media.as_ref().unwrap();
    assert_eq!(media.marker_variables.keys().map(String::as_str).collect::<Vec<_>>(), ["chatId"]);
    assert_eq!(media.tail_variables.keys().map(String::as_str).collect::<Vec<_>>(), ["chatId"]);
    for (case, source, schema, operations, message) in [
        ("operation", source.replace("marker \"chatMediaState\"", "marker \"Missing\""), schema.clone(), operations.clone(), "not a query"),
        ("path", source.replace("node.mediaLibrary", "node.wrong"), schema.clone(), operations.clone(), "wrong"),
        ("revision", source.clone(), schema.replace("revision: Int64!", "revision: String!"), operations.clone(), "non-null integer"),
        ("sequence", source.clone(), schema.replace("latestSequence: Int!", "latestSequence: Int"), operations.clone(), "non-null integer"),
        ("variable", source.clone(), schema.clone(), operations.replace("$afterSequence: Int", "$afterSequence: String"), "integer variable"),
        ("binding", source.clone(), schema.clone(), operations.replace("$chatId: ID!", "$otherId: ID!").replace("id: $chatId", "id: $otherId"), "parameter"),
        ("tail page", source.clone(), schema.clone(), operations.replace("hasNextPage endCursor", "hasNextPage"), "endCursor"),
        ("pair", source.replace("    tail \"chatMedia\", after: :afterSequence\n", ""), schema.clone(), operations.clone(), "paginated remote scope"),
    ] {
        let errors = compile(&source, &schema, &operations).expect_err(case);
        assert!(errors.iter().any(|error| error.error.message.contains(message)), "{case}: {errors:?}");
    }
}
