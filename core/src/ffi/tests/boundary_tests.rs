use super::{Config, Database};
use crate::ffi::DbError;
use crate::schema::{FieldSpec, ModelSpec, Storage};
use crate::value::ScalarKind;

async fn notes() -> std::sync::Arc<Database> {
    let mut model = ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    model.storage = Storage::Local;
    Database::open(Config { path: None, data_version: None }, crate::ffi::specs(serde_json::to_string(&vec![model]).unwrap()), crate::ffi::OpenPorts::default()).await.unwrap()
}

/// boundary_local_result_orders (TRACE-01, P-BND-03): the receipt of a local write shows the row of its own batch.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_local_writes_answer_with_the_row_of_their_own_batch() {
    let database = notes().await;
    let epoch = database.session_epoch();
    let made = database.create(epoch, "Note".into(), r#"{"text":"start"}"#.into(), "{}".into()).await.unwrap();
    let id = made.optimistic_value.unwrap().id;
    let (first, second) = tokio::join!(
        database.update(epoch, "Note".into(), id.clone(), r#"{"text":"first"}"#.into(), "{}".into()),
        database.update(epoch, "Note".into(), id.clone(), r#"{"text":"second"}"#.into(), "{}".into()),
    );
    let (first, second) = (first.unwrap(), second.unwrap());
    let shown = |receipt: &crate::ffi::receipt::WireOperationReceipt| format!("{:?}", receipt.optimistic_value.as_ref().unwrap().cells);
    assert!(shown(&first).contains("first") && !shown(&first).contains("second"), "{}", shown(&first));
    assert!(shown(&second).contains("second") && !shown(&second).contains("first"), "{}", shown(&second));
    assert_eq!(first.optimistic_value.as_ref().unwrap().revision, first.revision);
    assert_eq!(second.optimistic_value.as_ref().unwrap().revision, second.revision);
}

/// xfer_accept_row_missing (D-168): an upload to a row that is not stored fails with NotFound before the file is read.
#[tokio::test(flavor = "multi_thread")]
async fn an_upload_to_a_missing_row_is_not_found() {
    let database = notes().await;
    let request = crate::ffi::upload_wire::WireUploadRequest { path: "/nonexistent/file".into(), name: None, mime: None, variables_json: "{}".into() };
    let refused = database.upload(database.session_epoch(), "Note".into(), "absent".into(), "attach".into(), request).await;
    assert!(matches!(refused, Err(DbError::NotFound { .. })), "{refused:?}");
}

/// boundary_closed_call_precedence: after close an unknown operation is Closed, not Invalid.
#[tokio::test(flavor = "multi_thread")]
async fn a_call_after_close_is_closed_before_its_operation_is_checked() {
    let database = notes().await;
    let epoch = database.session_epoch();
    database.close_database().await.unwrap();
    let refused = database.call(epoch, "unknown".into(), "{}".into()).await;
    assert!(matches!(refused, Err(DbError::Closed { .. })), "{refused:?}");
}
