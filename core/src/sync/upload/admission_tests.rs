use serde_json::json;

use crate::db::Db;
use crate::sync::rig::*;
use crate::sync::{OperationDef, SyncConfig};
use crate::transport::MockTransport;
use crate::upload::{MultipartOperations, UploadBatchInput, UploadFile, UploadItem, UploadOperation, UploadOperations};

use super::upload_tests::{file, rid};

#[tokio::test(start_paused = true)]
async fn every_upload_entry_refuses_a_bad_declaration_without_touching_files_outbox_or_network() {
    let conflicting = UploadOperations {
        create_blob: Some(UploadOperation::rooted("CreateBlob", "a")),
        multipart: Some(MultipartOperations { initiate: UploadOperation::rooted("CreateBlob", "b"), presign: UploadOperation::new("CreateBlob"), complete: UploadOperation::new("CreateBlob"), abort: UploadOperation::new("CreateBlob") }),
    };
    let cases = [
        ("missing", None, "upload operations are not declared: pass UploadOperations to open"),
        ("empty", Some(UploadOperations::empty()), "upload operations declare neither createBlob nor multipart"),
        ("unknown", Some(UploadOperations::empty().create_blob("Missing")), "upload operation Missing is not registered"),
        ("conflicting", Some(conflicting), "upload operation CreateBlob is declared with different rootPath values"),
    ];
    let source = file(&vec![73; crate::files::TABLE_LIMIT + 1]);
    let path = source.path().to_str().unwrap();
    for (case, uploads, message) in cases {
        for entry in ["upload", "upload_create", "upload_create_batch"] {
            let directory = tempfile::tempdir().unwrap();
            let models = if entry == "upload_create_batch" { vec![super::batch_tests::spec()] } else { specs() };
            let rig = rig_on(Db::open(&directory.path().join("db.sqlite"), models, None).unwrap(), MockTransport::new(), SyncConfig { uploads: uploads.clone(), ..SyncConfig::default() });
            rig.sync.register(OperationDef::new("CreateBlob", "mutation x { x }", "createBlob"));
            put_if(&rig, entry);
            let outcome = match entry {
                "upload" => crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", UploadFile::new(path))).await.map(drop),
                "upload_create" => crate::sync::rig::app(&rig.sync, rig.sync.upload_create("moments", "upload", params(json!({"text": "x"})), UploadFile::new(path))).await.map(drop),
                _ => {
                    let items = (0..2).map(|position| UploadItem { values: params(json!({"clientKey": format!("key-{position}"), "size": 2})), path: path.into(), name: None, mime: None, variables: Default::default() }).collect();
                    crate::sync::rig::app(&rig.sync, rig.sync.upload_create_batch("Asset", items, UploadBatchInput { operation: "create_files".into(), variables: Default::default() })).await.map(drop)
                }
            };
            let error = outcome.expect_err(&format!("{case}/{entry} was accepted")).to_string();
            assert!(error.contains(&format!("invalid upload: {message}")), "{case}/{entry}: {error}");
            assert!(rig.db.shared.files.directory_entries().unwrap().is_empty(), "{case}/{entry}: a file was copied");
            assert!(rig.db.outbox().unwrap().is_empty(), "{case}/{entry}: an operation was accepted");
            assert!(rig.transport.calls().is_empty(), "{case}/{entry}: a request was sent");
        }
    }
}

fn put_if(rig: &Rig, entry: &str) {
    if entry == "upload" { put(rig, "moments", moment("m1", "x")); }
}
