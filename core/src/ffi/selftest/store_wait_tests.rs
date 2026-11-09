use crate::ffi::{Config, Database, DbError, QuerySpec};

async fn stand() -> (std::sync::Arc<Database>, u64, String, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("waits.sqlite");
    let mut spec = crate::schema::ModelSpec::new("Files", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id), crate::schema::FieldSpec::file("photo").nullable()]);
    spec.storage = crate::schema::Storage::Local;
    let models = serde_json::to_string(&vec![spec]).unwrap();
    let config = Config { path: Some(path.to_string_lossy().into()), data_version: None };
    let db = Database::open(config, crate::ffi::specs(models), crate::ffi::OpenPorts::default()).await.unwrap();
    let epoch = db.session_epoch();
    let id = db.create(epoch, "Files".into(), "{}".into(), "{}".into()).await.unwrap().optimistic_value.unwrap().id;
    db.attach(epoch, "Files".into(), id.clone(), "photo".into(), crate::ffi::FileBytes { bytes: vec![1, 2, 3], name: "small".into(), mime: "image/png".into() }).await.unwrap();
    (db, epoch, id, directory)
}

fn closed<T: std::fmt::Debug>(result: Result<T, DbError>) -> bool { matches!(result, Err(DbError::Closed { .. })) }
fn no_session<T: std::fmt::Debug>(result: Result<T, DbError>) -> bool { matches!(result, Err(DbError::NoSession { .. })) }

#[tokio::test(flavor = "multi_thread")]
async fn store_wait_peek_row() {
    let (db, epoch, id, _directory) = stand().await;
    assert!(db.peek_row(epoch, "Files".into(), id.clone()).unwrap().is_some());
    assert!(no_session(db.peek_row(epoch + 1, "Files".into(), id.clone())));
    db.close_database().await.unwrap();
    assert!(closed(db.peek_row(epoch, "Files".into(), id)));
}

#[tokio::test(flavor = "multi_thread")]
async fn store_wait_read_row() {
    let (db, epoch, id, _directory) = stand().await;
    assert!(db.read_row(epoch, "Files".into(), id.clone()).await.unwrap().is_some());
    assert!(no_session(db.read_row(epoch + 1, "Files".into(), id.clone()).await));
    db.close_database().await.unwrap();
    assert!(closed(db.read_row(epoch, "Files".into(), id).await));
}

fn all_files() -> QuerySpec {
    QuerySpec { model: "Files".into(), relation: None, params_json: "{}".into(), conditions: vec![], order: vec![], limit: None, offset: None, refresh_on_connect: false }
}

#[tokio::test(flavor = "multi_thread")]
async fn store_wait_read() {
    let (db, epoch, id, _directory) = stand().await;
    let rows = db.read_query(epoch, all_files()).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, id);
    assert!(no_session(db.read_query(epoch + 1, all_files()).await));
    db.close_database().await.unwrap();
    assert!(closed(db.read_query(epoch, all_files()).await));
}

#[tokio::test(flavor = "multi_thread")]
async fn store_wait_edge_values() {
    let (db, epoch, _id, _directory) = stand().await;
    assert!(no_session(db.edge_values(epoch + 1, "Files".into(), Some("feed".into()), "{}".into(), vec![]).await));
    db.close_database().await.unwrap();
    assert!(closed(db.edge_values(epoch, "Files".into(), Some("feed".into()), "{}".into(), vec![]).await));
}

#[tokio::test(flavor = "multi_thread")]
async fn store_wait_file() {
    let (db, epoch, id, _directory) = stand().await;
    assert_eq!(db.file(epoch, "Files".into(), id.clone(), "photo".into()).await.unwrap().unwrap().size, 3);
    assert!(no_session(db.file(epoch + 1, "Files".into(), id.clone(), "photo".into()).await));
    db.close_database().await.unwrap();
    assert!(closed(db.file(epoch, "Files".into(), id, "photo".into()).await));
}

#[tokio::test(flavor = "multi_thread")]
async fn store_wait_read_file() {
    let (db, epoch, id, _directory) = stand().await;
    assert_eq!(db.read(epoch, "Files".into(), id.clone(), "photo".into()).await.unwrap(), Some(vec![1, 2, 3]));
    assert!(no_session(db.read(epoch + 1, "Files".into(), id.clone(), "photo".into()).await));
    db.close_database().await.unwrap();
    assert!(closed(db.read(epoch, "Files".into(), id, "photo".into()).await));
}

#[tokio::test(flavor = "multi_thread")]
async fn store_wait_restore_failed() {
    let (db, _epoch, _id, _directory) = stand().await;
    assert!(db.open_report().unwrap().dropped.is_empty());
    db.close_database().await.unwrap();
    assert!(closed(db.open_report()));
}
