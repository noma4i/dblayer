use std::fs;
use std::process::Command;

#[test]
fn the_schema_check_reads_the_mutations_directory_beside_the_schema() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    fs::write(root.join("schema.graphql"), "type Query { a: Int }\ninput PostInput { file: String! }\ntype Mutation { post(input: PostInput!): Int }\n").unwrap();
    fs::create_dir(root.join("mutations")).unwrap();
    fs::write(root.join("mutations/post.graphql"), "mutation Post($input: PostInput!) { post(input: $input) }\n").unwrap();
    fs::write(root.join("post.dbl"), "model Post do\n  schema do\n    string :body\n  end\n\n  create with: Post, wrap: :input, input: { file: :file }, args: { file: :string? }\nend\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_dblayer-gen")).args(["--schema", "schema.graphql", "post.dbl"]).current_dir(root).output().unwrap();
    let errors = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success() && errors.contains("arg `file` may be nil but `file: String!` is required"), "{errors}");
}
