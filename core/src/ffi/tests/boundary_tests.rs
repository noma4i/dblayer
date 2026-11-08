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

/// boundary_local_result_orders (TRACE-01, P-BND-03): the answer of an attach shows the file of its own batch, and of two detaches exactly one had the file.
#[tokio::test(flavor = "multi_thread")]
async fn attach_and_detach_answer_with_their_own_batch() {
    let mut model = ModelSpec::new("Files", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::file("photo").nullable()]);
    model.storage = Storage::Local;
    let database = Database::open(Config { path: None, data_version: None }, crate::ffi::specs(serde_json::to_string(&vec![model]).unwrap()), crate::ffi::OpenPorts::default()).await.unwrap();
    let epoch = database.session_epoch();
    let id = database.create(epoch, "Files".into(), "{}".into(), "{}".into()).await.unwrap().optimistic_value.unwrap().id;
    let attached = database.attach(epoch, "Files".into(), id.clone(), "photo".into(), crate::ffi::FileBytes { bytes: vec![1, 2, 3], name: "first".into(), mime: "image/png".into() }).await.unwrap();
    assert_eq!((attached.name.as_deref(), attached.size), (Some("first"), 3));
    let (first, second) = tokio::join!(
        database.detach(epoch, "Files".into(), id.clone(), "photo".into()),
        database.detach(epoch, "Files".into(), id, "photo".into()),
    );
    let mut had = [first.unwrap(), second.unwrap()];
    had.sort();
    assert_eq!(had, [false, true]);
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
