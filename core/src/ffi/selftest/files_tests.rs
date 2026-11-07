use crate::ffi::{Config, Database, DbError};

#[tokio::test(flavor = "multi_thread")]
async fn store_wait_url() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("files.sqlite");
    let mut local = crate::schema::FieldSpec::object("draft").optional();
    local.fields = vec![crate::schema::FieldSpec::scalar("text", crate::value::ScalarKind::Str).optional()];
    local.merge = Some(crate::schema::MergeRule::Local);
    let mut spec = crate::schema::ModelSpec::new("Files", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id), crate::schema::FieldSpec::file("photo").nullable(), local]);
    spec.storage = crate::schema::Storage::Local;
    let models = serde_json::to_string(&vec![spec]).unwrap();
    let config = Config { path: Some(path.to_string_lossy().into()), data_version: None };
    let db = Database::open(config, crate::ffi::specs(models), crate::ffi::OpenPorts::default()).await.unwrap();
    let epoch = db.session_epoch();
    let id = db.create(epoch, "Files".into(), "{}".into(), "{}".into()).await.unwrap().optimistic_value.unwrap().id;
    assert_eq!(db.url(epoch, "Files".into(), id.clone(), "photo".into()).await.unwrap(), None);
    assert!(matches!(db.url(epoch, "Files".into(), id.clone(), "id".into()).await, Err(DbError::Invalid { .. })));
    for (path, value, expected) in [("photo", "null", "photo"), ("draft", "{\"unknown\":1}", "draft.unknown"), ("draft", "{\"text\":1}", "draft")] {
        let result = db.set_local(epoch, "Files".into(), id.clone(), path.into(), value.into()).await;
        assert!(matches!(result, Err(DbError::Invalid { detail }) if detail.contains(expected)));
    }
    assert!(matches!(db.url(epoch + 1, "Files".into(), id.clone(), "photo".into()).await, Err(DbError::NoSession { .. })));
    db.attach(epoch, "Files".into(), id.clone(), "photo".into(), crate::ffi::FileBytes { bytes: vec![7; 300 * 1024], name: "large".into(), mime: "image/png".into() }).await.unwrap();
    let url = db.url(epoch, "Files".into(), id.clone(), "photo".into()).await.unwrap().unwrap();
    assert_eq!(std::fs::read(url).unwrap(), vec![7; 300 * 1024]);
    db.close_database().await.unwrap();
    assert!(matches!(db.url(epoch, "Files".into(), id, "photo".into()).await, Err(DbError::Closed { .. })));
}

#[tokio::test(flavor = "multi_thread")]
async fn attached_files_survive_reopen_and_detaching_one_keeps_the_other() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("files.sqlite");
    let mut spec = crate::schema::ModelSpec::new("Files", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id), crate::schema::FieldSpec::file("photo").nullable()]);
    spec.storage = crate::schema::Storage::Local;
    let models = serde_json::to_string(&vec![spec]).unwrap();
    let config = || Config { path: Some(path.to_string_lossy().into()), data_version: None };
    let db = Database::open(config(), crate::ffi::specs(models.clone()), crate::ffi::OpenPorts::default()).await.unwrap();
    let epoch = db.session_epoch();
    let first = db.create(epoch, "Files".into(), "{}".into(), "{}".into()).await.unwrap().optimistic_value.unwrap().id;
    let second = db.create(epoch, "Files".into(), "{}".into(), "{}".into()).await.unwrap().optimistic_value.unwrap().id;
    let small = vec![1, 2, 3];
    let large = vec![9; 300 * 1024];
    db.attach(epoch, "Files".into(), first.clone(), "photo".into(), crate::ffi::FileBytes { bytes: small.clone(), name: "small".into(), mime: "image/png".into() }).await.unwrap();
    db.attach(epoch, "Files".into(), second.clone(), "photo".into(), crate::ffi::FileBytes { bytes: large.clone(), name: "large".into(), mime: "image/png".into() }).await.unwrap();
    assert_eq!(db.read(epoch, "Files".into(), first.clone(), "photo".into()).await.unwrap(), Some(small));
    let url = db.url(epoch, "Files".into(), second.clone(), "photo".into()).await.unwrap().unwrap();
    assert_eq!(std::fs::read(url).unwrap(), large);
    db.attach(epoch, "Files".into(), first.clone(), "photo".into(), crate::ffi::FileBytes { bytes: vec![4], name: "replacement".into(), mime: "image/png".into() }).await.unwrap();
    assert!(db.detach(epoch, "Files".into(), first.clone(), "photo".into()).await.unwrap());
    db.close_database().await.unwrap();
    drop(db);
    let db = Database::open(config(), crate::ffi::specs(models), crate::ffi::OpenPorts::default()).await.unwrap();
    let epoch = db.session_epoch();
    assert_eq!(db.read(epoch, "Files".into(), first, "photo".into()).await.unwrap(), None);
    assert_eq!(db.read(epoch, "Files".into(), second.clone(), "photo".into()).await.unwrap(), Some(large));
    let info = db.file(epoch, "Files".into(), second, "photo".into()).await.unwrap().unwrap();
    assert_eq!(info.name.as_deref(), Some("large"));
    assert_eq!(info.size, 300 * 1024);
    db.close_database().await.unwrap();
}
