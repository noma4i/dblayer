use std::process::Command;

#[test]
fn explain_runs_sqlite_plans_for_declared_scopes() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("notes.dbl");
    std::fs::write(&source, "model Note, storage: :local do\n  attribute :folderId, :id\n  attribute :position, :int\n  scope :in_folder do\n    parameter :folderId, :id\n    where folderId: :folderId\n    order position: :desc\n  end\nend\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_dblayer-gen")).arg("explain").arg(&source).output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let model = &report[0];
    assert_eq!(model["model"], "Note");
    let scope = &model["scopes"][0];
    assert_eq!(scope["name"], "in_folder");
    let plan = scope["plan"].as_array().unwrap().iter().map(|line| line.as_str().unwrap()).collect::<Vec<_>>().join("\n");
    assert!(plan.contains("USING INDEX") && !plan.contains("TEMP B-TREE"), "{plan}");
}
