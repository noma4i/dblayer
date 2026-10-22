use std::fs;
use std::process::Command;

#[test]
fn the_schema_check_reads_the_fragments_directory_beside_the_schema() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    fs::write(root.join("schema.graphql"), "type Query { items: ItemConnection! }\ntype ItemConnection { nodes: [Item!]! pageInfo: PageInfo! }\ntype PageInfo { hasNextPage: Boolean! }\ntype Item { id: ID! title: String }\n").unwrap();
    fs::create_dir(root.join("queries")).unwrap();
    fs::write(root.join("queries/items.graphql"), "query items { items { nodes { id ...Pieces } } }\n").unwrap();
    fs::create_dir(root.join("fragments")).unwrap();
    fs::write(root.join("fragments/pieces.graphql"), "fragment Pieces on Item { heading: title }\n").unwrap();
    fs::write(root.join("item.dbl"), "model Item do\n  attribute :caption, :string, null: true, from: \"heading\"\n  scope :all do\n    query \"items\"\n    paginate size: 20, direction: :forward\n  end\nend\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_dblayer-gen")).args(["--schema", "schema.graphql", "item.dbl"]).current_dir(root).output().unwrap();
    let errors = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{errors}");
}
