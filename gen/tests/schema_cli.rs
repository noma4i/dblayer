use std::fs;
use std::process::Command;

#[test]
fn the_schema_check_reads_the_mutations_directory_beside_the_schema() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    fs::write(root.join("schema.graphql"), "type Query { a: Int }\ninput PostInput { file: String! }\ntype Mutation { post(input: PostInput!): Int }\n").unwrap();
    fs::create_dir(root.join("mutations")).unwrap();
    fs::write(root.join("mutations/post.graphql"), "mutation Post($input: PostInput!) { post(input: $input) }\n").unwrap();
    fs::write(root.join("post.dbl"), "model Post do\n  attribute :body, :string\n  operation :create do\n    mutation \"Post\"\n    parameter :file, :string, null: true\n  end\nend\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_dblayer-gen")).args(["--schema", "schema.graphql", "post.dbl"]).current_dir(root).output().unwrap();
    let errors = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success() && errors.contains("parameter `file` may be nil but `file: String!` is required"), "{errors}");
}

#[test]
fn the_schema_flag_prints_the_input_objects_and_a_call_that_names_one_needs_it() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    fs::write(root.join("schema.graphql"), "type Query { a: Int }\ntype Mutation { stickerSend(input: StickerInput!): Int }\ninput StickerInput { name: String!, size: Int }\n").unwrap();
    fs::create_dir(root.join("mutations")).unwrap();
    fs::write(root.join("mutations/sticker.graphql"), "mutation StickerSend($input: StickerInput!) { stickerSend(input: $input) }").unwrap();
    fs::write(root.join("sticker.dbl"), "operation :sticker_send do\n  mutation \"StickerSend\"\n  parameter :input, StickerInput\nend\n").unwrap();
    let run = |arguments: &[&str]| Command::new(env!("CARGO_BIN_EXE_dblayer-gen")).args(arguments).current_dir(root).output().unwrap();
    let with = run(&["--schema", "schema.graphql", "swift", "sticker.dbl"]);
    let printed = String::from_utf8_lossy(&with.stdout);
    assert!(with.status.success() && printed.contains("public struct StickerInput: Encodable, Sendable, Equatable {"), "{printed}{}", String::from_utf8_lossy(&with.stderr));
    let without = run(&["kotlin", "sticker.dbl"]);
    let errors = String::from_utf8_lossy(&without.stderr);
    assert!(!without.status.success() && errors.contains("operation `sticker_send` needs `--schema`"), "{errors}");
}
