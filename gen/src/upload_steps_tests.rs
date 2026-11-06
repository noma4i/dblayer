use crate::parse_models;

const MODEL: &str = "model Clip do\n  attribute :title, :string, null: true\n  operation :send, upload: true do\n    mutation \"ClipSend\"\n    optimistic false\n    response from: \"clipSend\"\n  end\nend\n";

/// xfer_steps_declared_in_dbl: the steps come from one `uploads do` block and land on every model with an upload action.
#[test]
fn the_steps_of_uploads_are_declared_once_and_reach_the_models_with_an_upload_action() {
    let both = "uploads do\n  create_blob :CreateBlob, root: \"blobCreate\"\n  multipart initiate: :Init, presign: :Presign, complete: :Complete, abort: :Abort\nend\n";
    let plain = "model Note do\n  attribute :body, :string, null: true\nend\n";
    let models = parse_models(&format!("{both}{MODEL}{plain}")).unwrap();
    let steps = serde_json::to_value(models[0].upload_steps.as_ref().unwrap()).unwrap();
    assert_eq!(steps, serde_json::json!({"create_blob": {"name": "CreateBlob", "root_path": "blobCreate"}, "multipart": {"initiate": {"name": "Init"}, "presign": {"name": "Presign"}, "complete": {"name": "Complete"}, "abort": {"name": "Abort"}}}));
    assert!(models[1].upload_steps.is_none(), "a model without an upload action carries no steps");

    let cases = [
        ("upload action without the block", format!("{MODEL}"), "3:", "declare the steps of the upload once"),
        ("second block", format!("uploads do\n  create_blob :A\nend\nuploads do\n  create_blob :B\nend\n{MODEL}"), "4:", "declared once per project"),
        ("empty block", format!("uploads do\nend\n{MODEL}"), "1:", "declares neither `create_blob` nor `multipart`"),
        ("unknown step", format!("uploads do\n  presign :A\nend\n{MODEL}"), "2:", "knows `create_blob` and `multipart`"),
        ("multipart without abort", format!("uploads do\n  multipart initiate: :A, presign: :B, complete: :C\nend\n{MODEL}"), "2:", "needs `abort:`"),
        ("block without an upload action", format!("uploads do\n  create_blob :A\nend\n{plain}"), "1:", "no operation has `upload: true`"),
    ];
    for (case, source, line, message) in cases {
        let errors = parse_models(&source).unwrap_err();
        assert!(errors.iter().any(|error| format!("{}:", error.line) == line && error.message.contains(message)), "{case}: {errors:?}");
    }
}
