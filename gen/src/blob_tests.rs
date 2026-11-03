use std::fs;
use crate::{compile_files, emit_swift, emit_kotlin, emit_blob_swift, emit_blob_kotlin};

#[test]
fn blob_platforms() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().join("fixtures/operations");
    let source = fs::read_to_string(root.join("blob.dbl")).unwrap();
    let schema = fs::read_to_string(root.join("schema.graphql")).unwrap();
    let queries = fs::read_to_string(root.join("queries/blob.graphql")).unwrap();
    let compile = |source: &str, queries: &str| {
        let directory = tempfile::tempdir().unwrap();
        let model = directory.path().join("blob.dbl");
        let schema_file = directory.path().join("schema.graphql");
        let document = directory.path().join("queries.graphql");
        fs::write(&model, source).unwrap(); fs::write(&schema_file, &schema).unwrap(); fs::write(&document, queries).unwrap();
        compile_files(&[model], Some(&schema_file), &[document])
    };
    let declared = compile(&source, &queries).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &declared.inputs) + &emit_blob_swift(&declared.blob_uploads);
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &declared.inputs) + &emit_blob_kotlin(&declared.blob_uploads);
    assert!(swift.contains("static let videoIntro = BlobUpload(name: \"video_intro\", create: \"BlobCreate\")"), "{swift}");
    assert!(swift.contains("static func setVideo(video: Blob"), "{swift}");
    assert!(kotlin.contains("BlobUpload.Companion.VideoIntro"), "{kotlin}");
    assert!(kotlin.contains("suspend fun setVideo(video: Blob"), "{kotlin}");
    assert_eq!(declared.models[0].actions.iter().find(|action| action.name == "set_video").unwrap().variables["signedId"], "video");
    for (case, source, queries, message) in [
        ("unknown create", source.replace("create: \"BlobCreate\"", "create: \"Missing\""), queries.clone(), "not a mutation"),
        ("create input", source.clone(), queries.replace("$byteSize: Int!", "$byteSize: String!"), "byteSize"),
        ("consumer type", source.clone(), queries.replace("$signedId: String!", "$signedId: Int!"), "does not fit"),
        ("missing variable", source.clone(), queries.replace("$signedId: String!", "$other: String!").replace("signedId: $signedId", "signedId: $other"), "signedId"),
        ("duplicate", format!("{source}\nblob_upload :video_intro, create: \"BlobCreate\"\n"), queries.clone(), "twice"),
    ] {
        let errors = compile(&source, &queries).expect_err(case);
        assert!(errors.iter().any(|error| error.error.message.contains(message)), "{case}: {errors:?}");
    }
}
