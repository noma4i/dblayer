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
async fn boundary_local_result_orders() {
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
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), [1, 2, 3]).unwrap();
    let attached = database.attach_file(epoch, "Files".into(), id.clone(), "photo".into(), source.path().to_string_lossy().into(), "first".into(), "image/png".into()).await.unwrap();
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

/// boundary_close_lost_answer (P-BND-04, lifecycle.md Closing): a close whose answer is lost ends `Internal`, not `Ok`, and the resources of the database are released all the same.
#[tokio::test(flavor = "multi_thread")]
async fn boundary_close_lost_answer() {
    let database = notes().await;
    let epoch = database.session_epoch();
    database.life.outcomes().end_epoch(u64::MAX, crate::runtime::Cause::SessionReset);

    let closed = database.close_database().await;

    assert!(matches!(closed, Err(DbError::Internal { .. })), "{closed:?}");
    let refused = database.call(epoch, "unknown".into(), "{}".into()).await;
    assert!(matches!(refused, Err(DbError::Closed { .. })), "the database is shut down although the answer was lost: {refused:?}");
}

/// boundary_declaration_orders (P-BND-10, lifecycle.md RegisterOperations): in every order of two good declarations and one bad one the registration ends with the exact error and installs none of them, and the good ones alone are installed whole.
#[tokio::test(flavor = "multi_thread")]
async fn boundary_declaration_orders() {
    let good = |name: &str| serde_json::json!({"name": name, "document": format!("query {name} {{ {name} {{ id }} }}"), "rootPath": name});
    let bad = [
        (serde_json::json!({"name": "Bad", "rootPath": "bad"}), "operation needs document"),
        (serde_json::json!({"name": "Bad", "document": "query Bad { bad { id } }", "rootPath": "bad", "pageSize": "x"}), "operation Bad has invalid pageSize"),
        (serde_json::json!({"name": "Bad", "document": "{", "pageSize": 5}), "operation Bad has no root field"),
    ];
    let mut traces = 0;
    for (bad, detail) in bad {
        let (database, _transport) = super::close_tests::with_transport(std::sync::Arc::new(crate::runtime::ManualClock::new(std::time::Duration::from_secs(1)))).await;
        let declared = [good("Alpha"), good("Beta"), bad];
        for order in crate::test_wait::permutations(&[0, 1, 2]) {
            traces += 1;
            let defs = serde_json::Value::Array(order.iter().map(|at| declared[*at].clone()).collect()).to_string();
            let refused = database.register_operations(defs).await;
            assert!(matches!(&refused, Err(DbError::Invalid { detail: got }) if got == detail), "{order:?}: {refused:?}");
            assert!(database.reads.get("Alpha").is_none() && database.reads.get("Beta").is_none(), "{order:?}: a good declaration was installed beside the bad one");
            assert!(database.reads.get("Signal").is_some(), "{order:?}: the earlier declaration was kept");
        }
        database.register_operations(serde_json::Value::Array(vec![good("Alpha"), good("Beta"), good("Signal")]).to_string()).await.unwrap();
        assert!(database.reads.get("Alpha").is_some() && database.reads.get("Beta").is_some());
    }
    assert_eq!(traces, 18);
}
