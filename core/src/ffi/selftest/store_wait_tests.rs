use crate::ffi::{Config, Database, DbError, QuerySpec};

async fn stand() -> (std::sync::Arc<Database>, u64, String, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("waits.sqlite");
    let mut spec = crate::schema::ModelSpec::new("Files", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id), crate::schema::FieldSpec::file("photo").nullable()]);
    spec.storage = crate::schema::Storage::Local;
    let models = serde_json::to_string(&vec![spec]).unwrap();
    let config = Config { path: Some(path.to_string_lossy().into()) };
    let db = Database::open(config, crate::ffi::specs(models), crate::ffi::OpenPorts::default()).await.unwrap();
    let epoch = db.session_epoch();
    let id = db.create(epoch, "Files".into(), "{}".into(), "{}".into()).await.unwrap().optimistic_value.unwrap().id;
    let source = directory.path().join("source");
    std::fs::write(&source, [1, 2, 3]).unwrap();
    db.attach_file(epoch, "Files".into(), id.clone(), "photo".into(), source.to_string_lossy().into(), "small".into(), "image/png".into()).await.unwrap();
    (db, epoch, id, directory)
}

fn closed<T: std::fmt::Debug>(result: Result<T, DbError>) -> bool { matches!(result, Err(DbError::Closed { .. })) }
fn no_session<T: std::fmt::Debug>(result: Result<T, DbError>) -> bool { matches!(result, Err(DbError::NoSession { .. })) }

fn all_files() -> QuerySpec {
    QuerySpec { model: "Files".into(), relation: None, params_json: "{}".into(), conditions: vec![], order: vec![], limit: None, offset: None, refresh_on_connect: false }
}

#[tokio::test(flavor = "multi_thread")]
async fn every_store_read_answers_its_value_no_session_for_a_stale_epoch_and_closed_after_close() {
    let (db, epoch, id, _directory) = stand().await;
    assert!(db.peek_row(epoch, "Files".into(), id.clone()).await.unwrap().is_some());
    assert!(db.read_row(epoch, "Files".into(), id.clone()).await.unwrap().is_some());
    let rows = db.read_query(epoch, all_files()).await.unwrap();
    assert_eq!((rows.len(), rows[0].id.as_str()), (1, id.as_str()));
    assert_eq!(db.file(epoch, "Files".into(), id.clone(), "photo".into()).await.unwrap().unwrap().size, 3);
    assert!(db.open_report().unwrap().dropped.is_empty());
    assert!(no_session(db.peek_row(epoch + 1, "Files".into(), id.clone()).await));
    assert!(no_session(db.read_row(epoch + 1, "Files".into(), id.clone()).await));
    assert!(no_session(db.read_query(epoch + 1, all_files()).await));
    assert!(no_session(db.edge_values(epoch + 1, "Files".into(), Some("feed".into()), "{}".into(), vec![]).await));
    assert!(no_session(db.file(epoch + 1, "Files".into(), id.clone(), "photo".into()).await));
    db.close_database().await.unwrap();
    assert!(closed(db.peek_row(epoch, "Files".into(), id.clone()).await));
    assert!(closed(db.read_row(epoch, "Files".into(), id.clone()).await));
    assert!(closed(db.read_query(epoch, all_files()).await));
    assert!(closed(db.edge_values(epoch, "Files".into(), Some("feed".into()), "{}".into(), vec![]).await));
    assert!(closed(db.file(epoch, "Files".into(), id, "photo".into()).await));
    assert!(closed(db.open_report()));
}
